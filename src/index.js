'use strict';

import EventEmitter from 'event-emitter-mixin';
import KeyValueStore from 'key-value-store';
import sleep from 'sleep-promise';
import setImmediatePromise from 'set-immediate-promise';
import { flatten } from 'expand-flatten';
import clone from 'lodash.clone';
import difference from 'lodash.difference';
import isEmpty from 'lodash.isempty';
import isEqual from 'lodash.isequal';
import last from 'lodash.last';
import pull from 'lodash.pull';
import Collection from './collection';

let VERSION = 3;
const RESPIRATION_RATE = 250;

@EventEmitter
export class DocumentStore {
  constructor(options = {}) {
    if (!options.name) throw new Error('Document store name is missing');
    if (!options.url) throw new Error('Document store URL is missing');

    this.name = options.name;
    this.store = new KeyValueStore(options.url);

    this.collections = [];
    let collections = options.collections || [];
    for (let collection of collections) {
      this.addCollection(collection);
    }

    if (options.log) this.log = options.log;

    this.root = this;
  }

  use(plugin) {
    plugin.plug(this);
  }

  // === Document store ====

  async initializeDocumentStore() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.insideTransaction) {
      throw new Error('Cannot initialize the document store inside a transaction');
    }
    this.isInitializing = true;
    try {
      let hasBeenCreated = await this.createDocumentStoreIfDoesNotExist();
      if (!hasBeenCreated) {
        await this.lockDocumentStore();
        try {
          await this.upgradeDocumentStore();
          await this.verifyDocumentStore();
          await this.migrateDocumentStore();
        } finally {
          await this.unlockDocumentStore();
        }
      }
      this.hasBeenInitialized = true;
      await this.emit('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  }

  async createDocumentStoreIfDoesNotExist() {
    let hasBeenCreated = false;
    await this.store.transaction(async function(storeTransaction) {
      let record = await this._loadDocumentStoreRecord(storeTransaction, false);
      if (!record) {
        let collections = this.collections.map(collection => {
          return {
            name: collection.name,
            indexes: collection.indexes.map(index => index.name)
          };
        });
        record = {
          name: this.name,
          version: VERSION,
          collections
        };
        await this._saveDocumentStoreRecord(record, storeTransaction, true);
        hasBeenCreated = true;
        await this.emit('didCreate', storeTransaction);
        if (this.log) {
          this.log.info(`Document store '${this.name}' created`);
        }
      }
    }.bind(this));
    return hasBeenCreated;
  }

  async lockDocumentStore() {
    let hasBeenLocked = false;
    while (!hasBeenLocked) {
      await this.store.transaction(async function(storeTransaction) {
        let record = await this._loadDocumentStoreRecord(storeTransaction);
        if (!record.isLocked) {
          record.isLocked = hasBeenLocked = true;
          await this._saveDocumentStoreRecord(record, storeTransaction);
        }
      }.bind(this));
      if (!hasBeenLocked) {
        if (this.log) {
          this.log.info(`Waiting document store '${this.name}' unlocking...`);
        }
        await sleep(5000); // wait 5 secs before retrying
      }
    }
  }

  async unlockDocumentStore() {
    let record = await this._loadDocumentStoreRecord();
    record.isLocked = false;
    await this._saveDocumentStoreRecord(record);
  }

  async upgradeDocumentStore() {
    let record = await this._loadDocumentStoreRecord();
    let version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('Cannot downgrade the document store');
    }

    this.emit('upgradeDidStart');

    if (version < 2) {
      delete record.lastMigrationNumber;
      record.tables.forEach(table => {
        table.indexes = table.indexes.map(index => index.name);
      });
    }

    if (version < 3) { // rename 'tables' property to 'collections'
      record.collections = record.tables;
      delete record.tables;
    }

    record.version = VERSION;
    await this._saveDocumentStoreRecord(record);
    if (this.log) {
      this.log.info(`Document store '${this.name}' upgraded to version ${VERSION}`);
    }

    this.emit('upgradeDidStop');
  }

  async verifyDocumentStore() {
    // ...
  }

  async migrateDocumentStore() {
    let record = await this._loadDocumentStoreRecord();
    try {
      // Find out added or updated collections
      for (let collection of this.collections) {
        let existingCollection = record.collections.find(collec => collec.name === collection.name);
        if (!existingCollection) {
          this._emitMigrationDidStart();
          record.collections.push({
            name: collection.name,
            indexes: collection.indexes.map(index => index.name)
          });
          await this._saveDocumentStoreRecord(record);
          if (this.log) {
            this.log.info(`Collection '${collection.name}' (document store '${this.name}') added`);
          }
        } else if (existingCollection.hasBeenRemoved) {
          throw new Error('Adding a collection that has been removed is not implemented yet');
        } else {
          // Find out added indexes
          for (let index of collection.indexes) {
            if (!existingCollection.indexes.includes(index.name)) {
              this._emitMigrationDidStart();
              await this._addIndex(collection, index);
              existingCollection.indexes.push(index.name);
              await this._saveDocumentStoreRecord(record);
            }
          }
          // Find out removed indexes
          let existingIndexNames = clone(existingCollection.indexes);
          for (let existingIndexName of existingIndexNames) {
            if (!collection.indexes.find(index => index.name === existingIndexName)) {
              this._emitMigrationDidStart();
              await this._removeIndex(collection.name, existingIndexName);
              pull(existingCollection.indexes, existingIndexName);
              await this._saveDocumentStoreRecord(record);
            }
          }
        }
      }

      // Find out removed collections
      for (let existingCollection of record.collections) {
        if (existingCollection.hasBeenRemoved) continue;
        let collection = this.collections.find(collection => collection.name === existingCollection.name);
        if (!collection) {
          this._emitMigrationDidStart();
          for (let existingIndexName of existingCollection.indexes) {
            await this._removeIndex(existingCollection.name, existingIndexName);
          }
          existingCollection.indexes.length = 0;
          existingCollection.hasBeenRemoved = true;
          await this._saveDocumentStoreRecord(record);
          if (this.log) {
            this.log.info(`Collection '${existingCollection.name}' (document store '${this.name}') marked as removed`);
          }
        }
      }
    } finally {
      this._emitMigrationDidStop();
    }
  }

  _emitMigrationDidStart() {
    if (!this.migrationDidStartEventHasBeenEmitted) {
      this.emit('migrationDidStart');
      this.migrationDidStartEventHasBeenEmitted = true;
    }
  }

  _emitMigrationDidStop() {
    if (this.migrationDidStartEventHasBeenEmitted) {
      this.emit('migrationDidStop');
      delete this.migrationDidStartEventHasBeenEmitted;
    }
  }

  async _addIndex(collection, index) {
    if (this.log) {
      this.log.info(`Adding index '${index.name}' (document store '${this.name}', collection '${collection.name}')...`);
    }
    await this.forEachItems(collection, {}, async function(item, key) {
      await this.updateIndex(collection, key, undefined, item, index);
    }, this);
  }

  async _removeIndex(collectionName, indexName) {
    if (this.log) {
      this.log.info(`Removing index '${indexName}' (document store '${this.name}', collection '${collectionName}')...`);
    }
    let prefix = [this.name, this.makeIndexCollectionName(collectionName, indexName)];
    await this.store.delRange({ prefix });
  }

  async _loadDocumentStoreRecord(storeTransaction, errorIfMissing = true) {
    if (!storeTransaction) storeTransaction = this.store;
    return await storeTransaction.get([this.name], { errorIfMissing });
  }

  async _saveDocumentStoreRecord(record, storeTransaction, errorIfExists) {
    if (!storeTransaction) storeTransaction = this.store;
    await storeTransaction.put([this.name], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  }

  async getStatistics() {
    let collectionsCount = 0;
    let removedCollectionsCount = 0;
    let indexesCount = 0;
    let record = await this._loadDocumentStoreRecord(undefined, false);
    if (record) {
      record.collections.forEach(collection => {
        if (!collection.hasBeenRemoved) {
          collectionsCount++;
        } else {
          removedCollectionsCount++;
        }
        indexesCount += collection.indexes.length;
      });
    }
    let storePairsCount = await this.store.countRange({ prefix: this.name });
    return {
      collectionsCount,
      removedCollectionsCount,
      indexesCount,
      store: {
        pairsCount: storePairsCount
      }
    };
  }

  async removeCollectionsMarkedAsRemoved() {
    let record = await this._loadDocumentStoreRecord();
    let collectionNames = record.collections.map(collection => collection.name);
    for (let i = 0; i < collectionNames.length; i++) {
      let collectionName = collectionNames[i];
      let collection = record.collections.find(collection => collection.name === collectionName);
      if (!collection.hasBeenRemoved) continue;
      await this._removeCollection(collectionName);
      pull(record.collections, collection);
      await this._saveDocumentStoreRecord(record);
      if (this.log) {
        this.log.info(`Collection '${collectionName}' (document store '${this.name}') permanently removed`);
      }
    }
  }

  async _removeCollection(collectionName) {
    let prefix = [this.name, collectionName];
    await this.store.delRange({ prefix });
  }

  async destroyDocumentStore() {
    if (this.insideTransaction) {
      throw new Error('Cannot destroy a document store inside a transaction');
    }
    this.hasBeenInitialized = false;
    await this.store.delRange({ prefix: this.name });
  }

  async close() {
    await this.store.close();
  }

  // === Collections ====

  getCollection(name, errorIfMissing) {
    if (errorIfMissing == null) errorIfMissing = true;
    let collection = this.collections.find(collection => collection.name === name);
    if (!collection && errorIfMissing) {
      throw new Error(`Collection '${collection.name}' (document store '${this.name}') is missing`);
    }
    return collection;
  }

  addCollection(options = {}) {
    if (typeof options === 'string') options = { name: options };
    let collection = this.getCollection(options.name, false);
    if (collection) {
      throw new Error(`Collection '${options.name}' (document store '${this.name}') already exists`);
    }
    collection = new Collection(options);
    this.collections.push(collection);
  }

  normalizeCollection(collection) {
    if (typeof collection === 'string') collection = this.getCollection(collection);
    return collection;
  }

  // === Indexes ====

  async updateIndexes(collection, key, oldItem, newItem) {
    for (let i = 0; i < collection.indexes.length; i++) {
      let index = collection.indexes[i];
      await this.updateIndex(collection, key, oldItem, newItem, index);
    }
  }

  async updateIndex(collection, key, oldItem, newItem, index) {
    let flattenedOldItem = flatten(oldItem);
    let flattenedNewItem = flatten(newItem);
    let oldValues = [];
    let newValues = [];
    index.properties.forEach(property => {
      let oldValue, newValue;
      if (property.value === true) { // simple index
        oldValue = oldItem && flattenedOldItem[property.key];
        newValue = newItem && flattenedNewItem[property.key];
      } else { // computed index
        oldValue = oldItem && property.value(oldItem);
        newValue = newItem && property.value(newItem);
      }
      oldValues.push(oldValue);
      newValues.push(newValue);
    });
    let oldProjection;
    let newProjection;
    if (index.projection) {
      index.projection.forEach(k => {
        let val = flattenedOldItem[k];
        if (val != null) {
          if (!oldProjection) oldProjection = {};
          oldProjection[k] = val;
        }
        val = flattenedNewItem[k];
        if (val != null) {
          if (!newProjection) newProjection = {};
          newProjection[k] = val;
        }
      });
    }
    let valuesAreDifferent = !isEqual(oldValues, newValues);
    let projectionIsDifferent = !isEqual(oldProjection, newProjection);
    if (valuesAreDifferent && !oldValues.includes(undefined)) {
      let indexKey = this.makeIndexKey(collection, index, oldValues, key);
      await this.store.del(indexKey);
    }
    if ((valuesAreDifferent || projectionIsDifferent) && !newValues.includes(undefined)) {
      let indexKey = this.makeIndexKey(collection, index, newValues, key);
      await this.store.put(indexKey, newProjection);
    }
  }

  makeIndexKey(collection, index, values, key) {
    let indexKey = [this.name, this.makeIndexCollectionName(collection.name, index.name)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  }

  makeIndexCollectionName(collectionName, indexName) {
    return collectionName + ':' + indexName;
  }

  makeIndexKeyForQuery(collection, index, query) {
    if (!query) query = {};
    let indexKey = [this.name, this.makeIndexCollectionName(collection.name, index.name)];
    let keys = index.properties.map(property => property.key);
    let queryKeys = Object.keys(query);
    for (let i = 0; i < queryKeys.length; i++) {
      let key = keys[i];
      indexKey.push(query[key]);
    }
    return indexKey;
  }

  // === Basic operations ====

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. If an index projection matches
  //     the requested properties, the projection is used. Default: '*'.
  async getItem(collection, key, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    await this.initializeDocumentStore();
    let item = await this.store.get(this.makeItemKey(collection, key), options);
    return item;
  }

  // Options:
  //   createIfMissing: add the item if it is missing in the collection.
  //     If the item is already present, replace it. Default: true.
  //   errorIfExists: throw an error if the item is already present
  //     in the collection. Default: false.
  async putItem(collection, key, item, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    item = this.normalizeItem(item);
    options = this.normalizeOptions(options);
    await this.initializeDocumentStore();
    await this.transaction(async function(tr) {
      let itemKey = tr.makeItemKey(collection, key);
      let oldItem = await tr.store.get(itemKey, { errorIfMissing: false });
      await tr.store.put(itemKey, item, options);
      await tr.updateIndexes(collection, key, oldItem, item);
      await tr.emit('didPutItem', collection, key, item, options);
    });
  }

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  async deleteItem(collection, key, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    let hasBeenDeleted = false;
    await this.initializeDocumentStore();
    await this.transaction(async function(tr) {
      let itemKey = tr.makeItemKey(collection, key);
      let oldItem = await tr.store.get(itemKey, options);
      if (oldItem) {
        hasBeenDeleted = await tr.store.del(itemKey, options);
        await tr.updateIndexes(collection, key, oldItem, undefined);
        await tr.emit('didDeleteItem', collection, key, oldItem, options);
      }
    });
    return hasBeenDeleted;
  }

  // Options:
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. Default: '*'. TODO
  async getItems(collection, keys, options) {
    collection = this.normalizeCollection(collection);
    if (!Array.isArray(keys)) throw new Error('Invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeOptions(options);
    let itemKeys = keys.map(key => this.makeItemKey(collection, key));
    options = clone(options);
    options.returnValues = options.properties === '*' || options.properties.length;
    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.getMany(itemKeys, options);
    let finalItems = [];
    for (let item of items) {
      let finalItem = { key: last(item.key) };
      if (options.returnValues) finalItem.value = item.value;
      finalItems.push(finalItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return finalItems;
  }

  // Options:
  //   query: specifies the search query.
  //     Example: { blogId: 'xyz123', postId: 'abc987' }.
  //   order: specifies the property to order the results by:
  //     Example: ['lastName', 'firstName'].
  //   start, startAfter, end, endBefore: ...
  //   reverse: if true, the search is made in reverse order.
  //   properties: indicates properties to fetch. '*' for all properties
  //     or an array of property name. If an index projection matches
  //     the requested properties, the projection is used.
  //   limit: maximum number of items to return.
  async findItems(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!isEmpty(options.query) || !isEmpty(options.order)) {
      return await this._findItemsWithIndex(collection, options);
    }
    options = clone(options);
    options.prefix = [this.name, collection.name];
    options.returnValues = options.properties === '*' || options.properties.length;
    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.getRange(options);
    let finalItems = [];
    for (let item of items) {
      let finalItem = { key: last(item.key) };
      if (options.returnValues) finalItem.value = item.value;
      finalItems.push(finalItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return finalItems;
  }

  async _findItemsWithIndex(collection, options) {
    let index = collection.findIndexForQueryAndOrder(options.query, options.order);

    let fetchItem = options.properties === '*';
    let useProjection = false;
    if (!fetchItem && options.properties.length) {
      let diff = difference(options.properties, index.projection);
      useProjection = diff.length === 0;
      if (!useProjection) {
        fetchItem = true;
        if (this.log) {
          this.log.debug('An index projection doesn\'t satisfy requested properties, full item will be fetched');
        }
      }
    }

    options = clone(options);
    options.prefix = this.makeIndexKeyForQuery(collection, index, options.query);
    options.returnValues = useProjection;

    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.getRange(options);
    let transformedItems = [];
    for (let item of items) {
      let transformedItem = { key: last(item.key) };
      if (useProjection) transformedItem.value = item.value;
      transformedItems.push(transformedItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    items = transformedItems;

    if (fetchItem) {
      let keys = items.map(item => item.key);
      items = await this.getItems(collection, keys, { errorIfMissing: false });
    }

    return items;
  }

  // Options: same as findItems() without 'reverse' and 'properties' attributes.
  async countItems(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!isEmpty(options.query) || !isEmpty(options.order)) {
      return await this._countItemsWithIndex(collection, options);
    }
    options = clone(options);
    options.prefix = [this.name, collection.name];
    await this.initializeDocumentStore();
    return await this.store.countRange(options);
  }

  async _countItemsWithIndex(collection, options) {
    let index = collection.findIndexForQueryAndOrder(options.query, options.order);
    options = clone(options);
    options.prefix = this.makeIndexKeyForQuery(collection, index, options.query);
    await this.initializeDocumentStore();
    return await this.store.countRange(options);
  }

  // === Composed operations ===

  // Options: same as findItems() plus:
  //   batchSize: use several findItems() operations with batchSize as limit.
  //     Default: 250.
  async forEachItems(collection, options, fn, thisArg) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!options.batchSize) options.batchSize = 250;
    options = clone(options);
    options.limit = options.batchSize; // TODO: global 'limit' option
    while (true) {
      let items = await this.findItems(collection, options);
      if (!items.length) break;
      for (let i = 0; i < items.length; i++) {
        let item = items[i];
        await fn.call(thisArg, item.value, item.key);
      }
      let lastItem = last(items);
      options.startAfter = this.makeOrderKey(lastItem.key, lastItem.value, options.order);
      delete options.start;
    }
  }

  // Options: same as forEachItems() without 'properties' attribute.
  async findAndDeleteItems(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    options = clone(options);
    options.properties = [];
    let deletedItemsCount = 0;
    await this.forEachItems(collection, options, async function(value, key) {
      let hasBeenDeleted = await this.deleteItem(
        collection, key, { errorIfMissing: false }
      );
      if (hasBeenDeleted) deletedItemsCount++;
    }, this);
    return deletedItemsCount;
  }

  // === Transactions ====

  async transaction(fn) {
    if (this.insideTransaction) return await fn(this);
    await this.initializeDocumentStore();
    return await this.store.transaction(async function(storeTransaction) {
      let transaction = Object.create(this);
      transaction.store = storeTransaction;
      return await fn(transaction);
    }.bind(this));
  }

  get insideTransaction() {
    return this !== this.root;
  }

  // === Helpers ====

  makeItemKey(collection, key) {
    return [this.name, collection.name, key];
  }

  makeOrderKey(key, value, order = []) {
    let orderKey = order.map(k => value[k]);
    orderKey.push(key);
    return orderKey;
  }

  normalizeKey(key) {
    if (typeof key !== 'number' && typeof key !== 'string') {
      throw new Error('Invalid key type');
    }
    if (!key) {
      throw new Error('Specified key is null or empty');
    }
    return key;
  }

  normalizeItem(item) {
    if (!(item && typeof item === 'object')) throw new Error('Invalid item type');
    return item;
  }

  normalizeOptions(options) {
    if (!options) options = {};
    if (!options.hasOwnProperty('properties')) {
      options.properties = '*';
    } else if (options.properties === '*') {
      // It's OK
    } else if (Array.isArray(options.properties)) {
      // It's OK
    } else if (options.properties == null) {
      options.properties = [];
    } else {
      throw new Error('Invalid \'properties\' option');
    }
    return options;
  }
}

export default DocumentStore;
