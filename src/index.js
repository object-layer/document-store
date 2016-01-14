'use strict';

import assert from 'assert';
import EventEmitterMixin from 'event-emitter-mixin';
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

export class DocumentStore extends EventEmitterMixin() {
  constructor(options = {}) {
    super(options);

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
        let collections = this.collections.map(collection => collection.toJSON());
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

    this.emit('willUpgrade');

    if (version < 2) {
      delete record.lastMigrationNumber;
      record.tables.forEach(table => {
        table.indexes = table.indexes.map(index => index.name);
      });
    }

    if (version < 3) {
      throw new Error('Cannot upgrade the document store to version 3 automatically');
    }

    record.version = VERSION;
    await this._saveDocumentStoreRecord(record);
    if (this.log) {
      this.log.info(`Document store '${this.name}' upgraded to version ${VERSION}`);
    }

    this.emit('didUpgrade');
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
        if (!existingCollection) { // added collection
          this._emitMigrationDidStart();
          record.collections.push(collection.toJSON());
          await this._saveDocumentStoreRecord(record);
          if (this.log) {
            this.log.info(`Collection '${collection.name}' (document store '${this.name}') added`);
          }
        } else if (existingCollection.hasBeenRemoved) {
          throw new Error('Adding a collection that has been removed is not implemented yet');
        } else {
          // Find out added or updated indexes
          for (let index of collection.indexes) {
            let existingIndex = existingCollection.indexes.find(existingIndex => isEqual(existingIndex.keys, index.keys));
            if (!existingIndex) { // added index
              this._emitMigrationDidStart();
              await this._addIndex(collection, index);
              existingCollection.indexes.push(index.toJSON());
              await this._saveDocumentStoreRecord(record);
            } else if (!(isEqual(existingIndex.projection, index.projection) && existingIndex.version === index.version)) { // updated index
              this._emitMigrationDidStart();
              await this._updateIndex(collection, index);
              if (index.projection == null) delete existingIndex.projection;
              else existingIndex.projection = index.projection;
              if (index.version == null) delete existingIndex.version;
              else existingIndex.version = index.version;
              await this._saveDocumentStoreRecord(record);
            }
          }
          // Find out removed indexes
          let existingIndexesKeys = existingCollection.indexes.map(index => index.keys);
          for (let existingIndexKeys of existingIndexesKeys) {
            if (!collection.indexes.find(index => isEqual(index.keys, existingIndexKeys))) { // removed index
              this._emitMigrationDidStart();
              await this._removeIndex(collection.name, existingIndexKeys);
              let i = existingCollection.indexes.findIndex(index => isEqual(index.keys, existingIndexKeys));
              assert.notEqual(i, -1);
              existingCollection.indexes.splice(i, 1);
              await this._saveDocumentStoreRecord(record);
            }
          }
        }
      }

      // Find out removed collections
      for (let existingCollection of record.collections) {
        if (existingCollection.hasBeenRemoved) continue;
        let collection = this.collections.find(collection => collection.name === existingCollection.name);
        if (!collection) { // removed collection
          this._emitMigrationDidStart();
          for (let existingIndexes of existingCollection.indexes) {
            await this._removeIndex(existingCollection.name, existingIndexes.keys);
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
      this.emit('willMigrate');
      this.migrationDidStartEventHasBeenEmitted = true;
    }
  }

  _emitMigrationDidStop() {
    if (this.migrationDidStartEventHasBeenEmitted) {
      this.emit('didMigrate');
      delete this.migrationDidStartEventHasBeenEmitted;
    }
  }

  async _addIndex(collection, index) {
    let indexName = this.makeIndexName(index.keys);
    if (this.log) {
      this.log.info(`Adding index '${indexName}' (document store '${this.name}', collection '${collection.name}')...`);
    }
    await this.forEach(collection, {}, async function(doc, key) {
      console.log(doc, key);
      await this.updateIndex(collection, key, undefined, doc, index);
    }, this);
  }

  async _updateIndex(collection, index) {
    let indexName = this.makeIndexName(index.keys);
    if (this.log) {
      this.log.info(`Updating index '${indexName}' (document store '${this.name}', collection '${collection.name}')...`);
    }
    let prefix = [this.name, this.makeIndexCollectionName(collection.name, indexName)];
    await this.store.findAndDelete({ prefix });
    await this.forEach(collection, {}, async function(doc, key) {
      await this.updateIndex(collection, key, undefined, doc, index);
    }, this);
  }

  async _removeIndex(collectionName, indexKeys) {
    let indexName = this.makeIndexName(indexKeys);
    if (this.log) {
      this.log.info(`Removing index '${indexName}' (document store '${this.name}', collection '${collectionName}')...`);
    }
    let prefix = [this.name, this.makeIndexCollectionName(collectionName, indexName)];
    await this.store.findAndDelete({ prefix });
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
    let storePairsCount = await this.store.count({ prefix: this.name });
    return {
      collectionsCount,
      removedCollectionsCount,
      indexesCount,
      keyValueStore: {
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
    await this.store.findAndDelete({ prefix });
  }

  async destroyAll() {
    if (this.insideTransaction) {
      throw new Error('Cannot destroy a document store inside a transaction');
    }
    this.hasBeenInitialized = false;
    await this.store.findAndDelete({ prefix: this.name });
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
    let collection = new Collection(options);
    let existingCollection = this.getCollection(collection.name, false);
    if (existingCollection) {
      throw new Error(`Collection '${collection.name}' (document store '${this.name}') already exists`);
    }
    this.collections.push(collection);
  }

  normalizeCollection(collection) {
    if (typeof collection === 'string') collection = this.getCollection(collection);
    return collection;
  }

  // === Indexes ====

  async updateIndexes(collection, key, oldDoc, newDoc) {
    for (let i = 0; i < collection.indexes.length; i++) {
      let index = collection.indexes[i];
      await this.updateIndex(collection, key, oldDoc, newDoc, index);
    }
  }

  async updateIndex(collection, key, oldDoc, newDoc, index) {
    let flattenedOldDoc = flatten(oldDoc);
    let flattenedNewDoc = flatten(newDoc);
    let oldValues = [];
    let newValues = [];
    index.properties.forEach(property => {
      let oldValue, newValue;
      if (property.value === true) { // simple index
        oldValue = oldDoc && flattenedOldDoc[property.key];
        newValue = newDoc && flattenedNewDoc[property.key];
      } else { // computed index
        oldValue = oldDoc && property.value(oldDoc);
        newValue = newDoc && property.value(newDoc);
      }
      oldValues.push(oldValue);
      newValues.push(newValue);
    });
    let oldProjection;
    let newProjection;
    if (index.projection) {
      index.projection.forEach(k => {
        let val = flattenedOldDoc[k];
        if (val != null) {
          if (!oldProjection) oldProjection = {};
          oldProjection[k] = val;
        }
        val = flattenedNewDoc[k];
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
      await this.store.delete(indexKey);
    }
    if ((valuesAreDifferent || projectionIsDifferent) && !newValues.includes(undefined)) {
      let indexKey = this.makeIndexKey(collection, index, newValues, key);
      await this.store.put(indexKey, newProjection);
    }
  }

  makeIndexName(keys) {
    return keys.join('+');
  }

  makeIndexKey(collection, index, values, key) {
    let indexName = this.makeIndexName(index.keys);
    let indexKey = [this.name, this.makeIndexCollectionName(collection.name, indexName)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  }

  makeIndexCollectionName(collectionName, indexName) {
    return collectionName + ':' + indexName;
  }

  makeIndexKeyForQuery(collection, index, query) {
    if (!query) query = {};
    let indexName = this.makeIndexName(index.keys);
    let indexKey = [this.name, this.makeIndexCollectionName(collection.name, indexName)];
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
  //   errorIfMissing: throw an error if the document is not found. Default: true.
  async get(collection, key, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    await this.initializeDocumentStore();
    let doc = await this.store.get(this.makeDocumentKey(collection, key), options);
    return doc;
  }

  // Options:
  //   createIfMissing: add the document if it is missing in the collection.
  //     If the document is already present, replace it. Default: true.
  //   errorIfExists: throw an error if the document is already present
  //     in the collection. Default: false.
  async put(collection, key, doc, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    doc = this.normalizeDocument(doc);
    options = this.normalizeOptions(options);
    await this.initializeDocumentStore();
    let oldDoc;
    await this.transaction(async function(tr) {
      let docKey = tr.makeDocumentKey(collection, key);
      oldDoc = await tr.store.get(docKey, { errorIfMissing: false });
      await tr.emit('willPut', collection, key, oldDoc, doc, options);
      await tr.store.put(docKey, doc, options);
      await tr.updateIndexes(collection, key, oldDoc, doc);
    });
    await this.emit('didPut', collection, key, oldDoc, doc, options);
  }

  // Options:
  //   errorIfMissing: throw an error if the document is not found. Default: true.
  async delete(collection, key, options) {
    collection = this.normalizeCollection(collection);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    let hasBeenDeleted = false;
    await this.initializeDocumentStore();
    let oldDoc;
    await this.transaction(async function(tr) {
      let docKey = tr.makeDocumentKey(collection, key);
      oldDoc = await tr.store.get(docKey, options);
      if (oldDoc) {
        await tr.emit('willDelete', collection, key, oldDoc, options);
        hasBeenDeleted = await tr.store.delete(docKey, options);
        await tr.updateIndexes(collection, key, oldDoc, undefined);
      }
    });
    if (oldDoc) {
      await this.emit('didDelete', collection, key, oldDoc, options);
    }
    return hasBeenDeleted;
  }

  async getMany(collection, keys, options) {
    collection = this.normalizeCollection(collection);
    if (!Array.isArray(keys)) throw new Error('Invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeOptions(options);
    let docKeys = keys.map(key => this.makeDocumentKey(collection, key));
    options = clone(options);
    options.returnValues = options.properties === '*' || options.properties.length;
    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.getMany(docKeys, options);
    let finalItems = [];
    for (let item of items) {
      let finalItem = { key: last(item.key) };
      if (options.returnValues) finalItem.document = item.value;
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
  //   limit: maximum number of documents to return.
  async find(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!isEmpty(options.query) || !isEmpty(options.order)) {
      return await this._findWithIndex(collection, options);
    }
    options = clone(options);
    options.prefix = [this.name, collection.name];
    options.returnValues = options.properties === '*' || options.properties.length;
    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.find(options);
    let finalItems = [];
    for (let item of items) {
      let finalItem = { key: last(item.key) };
      if (options.returnValues) finalItem.document = item.value;
      finalItems.push(finalItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return finalItems;
  }

  async _findWithIndex(collection, options) {
    let index = collection.findIndexForQueryAndOrder(options.query, options.order);

    let fetchDoc = options.properties === '*';
    let useProjection = false;
    if (!fetchDoc && options.properties.length) {
      let diff = difference(options.properties, index.projection);
      useProjection = diff.length === 0;
      if (!useProjection) {
        fetchDoc = true;
        if (this.log) {
          this.log.debug('An index projection doesn\'t satisfy requested properties, full documents will be fetched');
        }
      }
    }

    options = clone(options);
    options.prefix = this.makeIndexKeyForQuery(collection, index, options.query);
    options.returnValues = useProjection;

    let iterationsCount = 0;
    await this.initializeDocumentStore();
    let items = await this.store.find(options);
    let transformedItems = [];
    for (let item of items) {
      let transformedItem = { key: last(item.key) };
      if (useProjection) transformedItem.document = item.value;
      transformedItems.push(transformedItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    items = transformedItems;

    if (fetchDoc) {
      let keys = items.map(item => item.key);
      items = await this.getMany(collection, keys, { errorIfMissing: false });
    }

    return items;
  }

  // Options: same as find() without 'reverse' and 'properties' attributes.
  async count(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!isEmpty(options.query) || !isEmpty(options.order)) {
      return await this._countWithIndex(collection, options);
    }
    options = clone(options);
    options.prefix = [this.name, collection.name];
    await this.initializeDocumentStore();
    return await this.store.count(options);
  }

  async _countWithIndex(collection, options) {
    let index = collection.findIndexForQueryAndOrder(options.query, options.order);
    options = clone(options);
    options.prefix = this.makeIndexKeyForQuery(collection, index, options.query);
    await this.initializeDocumentStore();
    return await this.store.count(options);
  }

  // === Composed operations ===

  // Options: same as find() plus:
  //   batchSize: use several find() operations with batchSize as limit.
  //     Default: 250.
  async forEach(collection, options, fn, thisArg) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    if (!options.batchSize) options.batchSize = 250;
    options = clone(options);
    options.limit = options.batchSize; // TODO: global 'limit' option
    while (true) {
      let items = await this.find(collection, options);
      if (!items.length) break;
      for (let i = 0; i < items.length; i++) {
        let item = items[i];
        await fn.call(thisArg, item.document, item.key);
      }
      let lastItem = last(items);
      options.startAfter = this.makeOrderKey(lastItem.key, lastItem.document, options.order);
      delete options.start;
    }
  }

  // Options: same as forEach() without 'properties' attribute.
  async findAndDelete(collection, options) {
    collection = this.normalizeCollection(collection);
    options = this.normalizeOptions(options);
    options = clone(options);
    options.properties = [];
    let deletedDocsCount = 0;
    await this.forEach(collection, options, async function(doc, key) {
      let hasBeenDeleted = await this.delete(
        collection, key, { errorIfMissing: false }
      );
      if (hasBeenDeleted) deletedDocsCount++;
    }, this);
    return deletedDocsCount;
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

  makeDocumentKey(collection, key) {
    return [this.name, collection.name, key];
  }

  makeOrderKey(key, doc, order = []) {
    let orderKey = order.map(k => doc[k]);
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

  normalizeDocument(doc) {
    if (!(doc && typeof doc === 'object')) throw new Error('Invalid document type');
    return doc;
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
