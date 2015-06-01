'use strict';

let _ = require('lodash');
let wait = require('co-wait');
let KindaObject = require('kinda-object');
let KindaEventManager = require('kinda-event-manager');
let util = require('kinda-util').create();
let KindaLog = require('kinda-log');
let Store = require('kinda-store');
let Table = require('./table');

let VERSION = 2;

let KindaDB = KindaObject.extend('KindaDB', function() {
  this.include(KindaEventManager);

  this.creator = function(options = {}) {
    if (!options.name) throw new Error('database name is missing');
    if (!options.url) throw new Error('database url is missing');

    let log = options.log;
    if (!KindaLog.isClassOf(log)) log = KindaLog.create(log);
    this.log = log;

    this.name = options.name;
    this.store = Store.create({ url: options.url });
    this.tables = [];
    (options.tables || []).forEach(table => {
      if (_.isString(table)) table = { name: table };
      this.addTable(table);
    });

    this.database = this;
  };

  this.use = function(plugin) {
    plugin.plug(this);
  };

  // === Database ====

  this.initializeDatabase = function *() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.isInsideTransaction()) {
      throw new Error('cannot initialize the database inside a transaction');
    }
    this.isInitializing = true;
    try {
      let hasBeenCreated = yield this.createDatabaseIfDoesNotExist();
      if (!hasBeenCreated) {
        yield this.lockDatabase();
        try {
          yield this.upgradeDatabase();
          yield this.verifyDatabase();
          yield this.migrateDatabase();
        } finally {
          yield this.unlockDatabase();
        }
      }
      this.hasBeenInitialized = true;
      yield this.emitAsync('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  };

  this.loadDatabaseRecord = function *(tr, errorIfMissing) {
    if (!tr) tr = this.store;
    if (errorIfMissing == null) errorIfMissing = true;
    return yield tr.get([this.name], { errorIfMissing });
  };

  this.saveDatabaseRecord = function *(record, tr, errorIfExists) {
    if (!tr) tr = this.store;
    yield tr.put([this.name], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createDatabaseIfDoesNotExist = function *() {
    let hasBeenCreated = false;
    yield this.store.transaction(function *(tr) {
      let record = yield this.loadDatabaseRecord(tr, false);
      if (!record) {
        let tables = this.tables.map(table => {
          return {
            name: table.name,
            indexes: _.pluck(table.indexes, 'name')
          };
        });
        record = {
          name: this.name,
          version: VERSION,
          tables
        };
        yield this.saveDatabaseRecord(record, tr, true);
        hasBeenCreated = true;
        yield this.emitAsync('didCreate', tr);
        this.log.info(`Database '${this.name}' created`);
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.lockDatabase = function *() {
    let hasBeenLocked = false;
    while (!hasBeenLocked) {
      yield this.store.transaction(function *(tr) {
        let record = yield this.loadDatabaseRecord(tr);
        if (!record.isLocked) {
          record.isLocked = hasBeenLocked = true;
          yield this.saveDatabaseRecord(record, tr);
        }
      }.bind(this));
      if (!hasBeenLocked) {
        this.log.info(`Waiting database '${this.name}'...`);
        yield wait(5000); // wait 5 secs before retrying
      }
    }
  };

  this.unlockDatabase = function *() {
    let record = yield this.loadDatabaseRecord();
    record.isLocked = false;
    yield this.saveDatabaseRecord(record);
  };

  this.upgradeDatabase = function *() {
    let record = yield this.loadDatabaseRecord();
    let version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('cannot downgrade the database');
    }

    this.emit('upgradeDidStart');

    if (version < 2) {
      delete record.lastMigrationNumber;
      record.tables.forEach(table => {
        table.indexes = _.pluck(table.indexes, 'name');
      });
    }

    record.version = VERSION;
    yield this.saveDatabaseRecord(record);
    this.log.info(`Database '${this.name}' upgraded to version ${VERSION}`);

    this.emit('upgradeDidStop');
  };

  this.verifyDatabase = function *() {
    // ...
  };

  this.migrateDatabase = function *() {
    let record = yield this.loadDatabaseRecord();
    try {
      // Find out added or updated tables
      for (let table of this.tables) {
        let existingTable = _.find(record.tables, 'name', table.name);
        if (!existingTable) {
          this._emitMigrationDidStart();
          record.tables.push({
            name: table.name,
            indexes: _.pluck(table.indexes, 'name')
          });
          yield this.saveDatabaseRecord(record);
          this.log.info(`Table '${table.name}' (database '${this.name}') added`);
        } else if (existingTable.hasBeenRemoved) {
          throw new Error('adding a table that has been removed is not implemented yet');
        } else {
          // Find out added indexes
          for (let index of table.indexes) {
            if (!_.contains(existingTable.indexes, index.name)) {
              this._emitMigrationDidStart();
              yield this._addIndex(table, index);
              existingTable.indexes.push(index.name);
              yield this.saveDatabaseRecord(record);
            }
          }
          // Find out removed indexes
          let existingIndexNames = _.clone(existingTable.indexes);
          for (let existingIndexName of existingIndexNames) {
            if (!_.find(table.indexes, 'name', existingIndexName)) {
              this._emitMigrationDidStart();
              yield this._removeIndex(table.name, existingIndexName);
              _.pull(existingTable.indexes, existingIndexName);
              yield this.saveDatabaseRecord(record);
            }
          }
        }
      }

      // Find out removed tables
      for (let existingTable of record.tables) {
        if (existingTable.hasBeenRemoved) continue;
        let table = _.find(this.tables, 'name', existingTable.name);
        if (!table) {
          this._emitMigrationDidStart();
          for (let existingIndexName of existingTable.indexes) {
            yield this._removeIndex(existingTable.name, existingIndexName);
          }
          existingTable.indexes.length = 0;
          existingTable.hasBeenRemoved = true;
          yield this.saveDatabaseRecord(record);
          this.log.info(`Table '${existingTable.name}' (database '${this.name}') marked as removed`);
        }
      }
    } finally {
      this._emitMigrationDidStop();
    }
  };

  this._emitMigrationDidStart = function() {
    if (!this.migrationDidStartEventHasBeenEmitted) {
      this.emit('migrationDidStart');
      this.migrationDidStartEventHasBeenEmitted = true;
    }
  };

  this._emitMigrationDidStop = function() {
    if (this.migrationDidStartEventHasBeenEmitted) {
      this.emit('migrationDidStop');
      delete this.migrationDidStartEventHasBeenEmitted;
    }
  };

  this._addIndex = function *(table, index) {
    this.log.info(`Adding index '${index.name}' (database '${this.name}', table '${table.name}')...`);
    yield this.forEachItems(table, {}, function *(item, key) {
      yield this.updateIndex(table, key, undefined, item, index);
    }, this);
  };

  this._removeIndex = function *(tableName, indexName) {
    this.log.info(`Removing index '${indexName}' (database '${this.name}', table '${tableName}')...`);
    let prefix = [this.name, this.makeIndexTableName(tableName, indexName)];
    yield this.store.delRange({ prefix });
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction()) return yield fn(this);
    yield this.initializeDatabase();
    return yield this.store.transaction(function *(tr) {
      let transaction = Object.create(this);
      transaction.store = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.isInsideTransaction = function() {
    return this !== this.database;
  };

  this.getStatistics = function *() {
    let tablesCount = 0;
    let removedTablesCount = 0;
    let indexesCount = 0;
    let record = yield this.loadDatabaseRecord(undefined, false);
    if (record) {
      record.tables.forEach(table => {
        if (!table.hasBeenRemoved) {
          tablesCount++;
        } else {
          removedTablesCount++;
        }
        indexesCount += table.indexes.length;
      });
    }
    let storePairsCount = yield this.store.getCount({ prefix: this.name });
    return {
      tablesCount,
      removedTablesCount,
      indexesCount,
      store: {
        pairsCount: storePairsCount
      }
    };
  };

  this.removeTablesMarkedAsRemoved = function *() {
    let record = yield this.loadDatabaseRecord();
    let tableNames = _.pluck(record.tables, 'name');
    for (let i = 0; i < tableNames.length; i++) {
      let tableName = tableNames[i];
      let table = _.find(record.tables, 'name', tableName);
      if (!table.hasBeenRemoved) continue;
      yield this._removeTable(tableName);
      _.pull(record.tables, table);
      yield this.saveDatabaseRecord(record);
      this.log.info(`Table '${tableName}' (database '${this.name}') permanently removed`);
    }
  };

  this._removeTable = function *(tableName) {
    let prefix = [this.name, tableName];
    yield this.store.delRange({ prefix });
  };

  this.destroyDatabase = function *() {
    if (this.isInsideTransaction()) {
      throw new Error('cannot reset the database inside a transaction');
    }
    this.hasBeenInitialized = false;
    yield this.store.delRange({ prefix: this.name });
  };

  this.closeDatabase = function *() {
    yield this.store.close();
  };

  // === Tables ====

  this.getTable = function(name, errorIfMissing) {
    if (errorIfMissing == null) errorIfMissing = true;
    let table = _.find(this.tables, 'name', name);
    if (!table && errorIfMissing) {
      throw new Error(`Table '${table.name}' (database '${this.name}') is missing`);
    }
    return table;
  };

  this.addTable = function(options = {}) {
    let table = this.getTable(options.name, false);
    if (table) {
      throw new Error(`Table '${options.name}' (database '${this.name}') already exists`);
    }
    table = Table.create(options);
    this.tables.push(table);
  };

  this.normalizeTable = function(table) {
    if (_.isString(table)) table = this.getTable(table);
    return table;
  };

  // === Indexes ====

  this.updateIndexes = function *(table, key, oldItem, newItem) {
    for (let i = 0; i < table.indexes.length; i++) {
      let index = table.indexes[i];
      yield this.updateIndex(table, key, oldItem, newItem, index);
    }
  };

  this.updateIndex = function *(table, key, oldItem, newItem, index) {
    let flattenedOldItem = util.flattenObject(oldItem);
    let flattenedNewItem = util.flattenObject(newItem);
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
    let valuesAreDifferent = !_.isEqual(oldValues, newValues);
    let projectionIsDifferent = !_.isEqual(oldProjection, newProjection);
    if (valuesAreDifferent && !_.contains(oldValues, undefined)) {
      let indexKey = this.makeIndexKey(table, index, oldValues, key);
      yield this.store.del(indexKey);
    }
    if ((valuesAreDifferent || projectionIsDifferent) && !_.contains(newValues, undefined)) {
      let indexKey = this.makeIndexKey(table, index, newValues, key);
      yield this.store.put(indexKey, newProjection);
    }
  };

  this.makeIndexKey = function(table, index, values, key) {
    let indexKey = [this.name, this.makeIndexTableName(table.name, index.name)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  };

  this.makeIndexTableName = function(tableName, indexName) {
    return tableName + ':' + indexName;
  };

  this.makeIndexKeyForQuery = function(table, index, query) {
    if (!query) query = {};
    let indexKey = [this.name, this.makeIndexTableName(table.name, index.name)];
    let keys = _.pluck(index.properties, 'key');
    let queryKeys = _.keys(query);
    for (let i = 0; i < queryKeys.length; i++) {
      let key = keys[i];
      indexKey.push(query[key]);
    }
    return indexKey;
  };

  // === Basic operations ====

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. If an index projection matches
  //     the requested properties, the projection is used. Default: '*'.
  this.getItem = function *(table, key, options) {
    table = this.normalizeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    yield this.initializeDatabase();
    let item = yield this.store.get(this.makeItemKey(table, key), options);
    return item;
  };

  // Options:
  //   createIfMissing: add the item if it is missing in the table.
  //     If the item is already present, replace it. Default: true.
  //   errorIfExists: throw an error if the item is already present
  //     in the table. Default: false.
  this.putItem = function *(table, key, item, options) {
    table = this.normalizeTable(table);
    key = this.normalizeKey(key);
    item = this.normalizeItem(item);
    options = this.normalizeOptions(options);
    yield this.initializeDatabase();
    yield this.transaction(function *(tr) {
      let itemKey = tr.makeItemKey(table, key);
      let oldItem = yield tr.store.get(itemKey, { errorIfMissing: false });
      yield tr.store.put(itemKey, item, options);
      yield tr.updateIndexes(table, key, oldItem, item);
      yield tr.emitAsync('didPutItem', table, key, item, options);
    });
  };

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  this.deleteItem = function *(table, key, options) {
    table = this.normalizeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    let hasBeenDeleted = false;
    yield this.initializeDatabase();
    yield this.transaction(function *(tr) {
      let itemKey = tr.makeItemKey(table, key);
      let oldItem = yield tr.store.get(itemKey, options);
      if (oldItem) {
        hasBeenDeleted = yield tr.store.del(itemKey, options);
        yield tr.updateIndexes(table, key, oldItem, undefined);
        yield tr.emitAsync('didDeleteItem', table, key, oldItem, options);
      }
    });
    return hasBeenDeleted;
  };

  // Options:
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. Default: '*'. TODO
  this.getItems = function *(table, keys, options) {
    table = this.normalizeTable(table);
    if (!_.isArray(keys)) throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeOptions(options);
    let itemKeys = keys.map(key => this.makeItemKey(table, key));
    options = _.clone(options);
    options.returnValues = options.properties === '*' || options.properties.length;
    yield this.initializeDatabase();
    let items = yield this.store.getMany(itemKeys, options);
    items = items.map(item => {
      let res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

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
  this.findItems = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    if (!_.isEmpty(options.query) || !_.isEmpty(options.order)) {
      return yield this._findItemsWithIndex(table, options);
    }
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    options.returnValues = options.properties === '*' || options.properties.length;
    yield this.initializeDatabase();
    let items = yield this.store.getRange(options);
    items = items.map(item => {
      let res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  this._findItemsWithIndex = function *(table, options) {
    let index = table.findIndexForQueryAndOrder(options.query, options.order);

    let fetchItem = options.properties === '*';
    let useProjection = false;
    if (!fetchItem && options.properties.length) {
      let diff = _.difference(options.properties, index.projection);
      useProjection = diff.length === 0;
      if (!useProjection) {
        fetchItem = true;
        this.log.debug('an index projection doesn\'t satisfy requested properties, full item will be fetched');
      }
    }

    options = _.clone(options);
    options.prefix = this.makeIndexKeyForQuery(table, index, options.query);
    options.returnValues = useProjection;

    yield this.initializeDatabase();
    let items = yield this.store.getRange(options);
    items = items.map(item => {
      let res = { key: _.last(item.key) };
      if (useProjection) res.value = item.value;
      return res;
    });

    if (fetchItem) {
      let keys = _.pluck(items, 'key');
      items = yield this.getItems(table, keys, { errorIfMissing: false });
    }

    return items;
  };

  // Options: same as findItems() without 'reverse' and 'properties' attributes.
  this.countItems = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    if (!_.isEmpty(options.query) || !_.isEmpty(options.order)) {
      return yield this._countItemsWithIndex(table, options);
    }
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    yield this.initializeDatabase();
    return yield this.store.getCount(options);
  };

  this._countItemsWithIndex = function *(table, options) {
    let index = table.findIndexForQueryAndOrder(options.query, options.order);
    options = _.clone(options);
    options.prefix = this.makeIndexKeyForQuery(table, index, options.query);
    yield this.initializeDatabase();
    return yield this.store.getCount(options);
  };

  // === Composed operations ===

  // Options: same as findItems() plus:
  //   batchSize: use several findItems() operations with batchSize as limit.
  //     Default: 250.
  this.forEachItems = function *(table, options, fn, thisArg) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    if (!options.batchSize) options.batchSize = 250;
    options = _.clone(options);
    options.limit = options.batchSize; // TODO: global 'limit' option
    while (true) {
      let items = yield this.findItems(table, options);
      if (!items.length) break;
      for (let i = 0; i < items.length; i++) {
        let item = items[i];
        yield fn.call(thisArg, item.value, item.key);
      }
      let lastItem = _.last(items);
      options.startAfter = this.makeOrderKey(lastItem.key, lastItem.value, options.order);
      delete options.start;
    }
  };

  // Options: same as forEachItems() without 'properties' attribute.
  this.findAndDeleteItems = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    options = _.clone(options);
    options.properties = [];
    let deletedItemsCount = 0;
    yield this.forEachItems(table, options, function *(value, key) {
      let hasBeenDeleted = yield this.deleteItem(
        table, key, { errorIfMissing: false }
      );
      if (hasBeenDeleted) deletedItemsCount++;
    }, this);
    return deletedItemsCount;
  };

  // === Helpers ====

  this.makeItemKey = function(table, key) {
    return [this.name, table.name, key];
  };

  this.makeOrderKey = function(key, value, order = []) {
    let orderKey = order.map(k => value[k]);
    orderKey.push(key);
    return orderKey;
  };

  this.normalizeKey = function(key) {
    if (typeof key !== 'number' && typeof key !== 'string') {
      throw new Error('invalid key type');
    }
    if (!key) {
      throw new Error('key is null or empty');
    }
    return key;
  };

  this.normalizeItem = function(item) {
    if (!_.isObject(item)) throw new Error('invalid item type');
    return item;
  };

  this.normalizeOptions = function(options) {
    if (!options) options = {};
    if (options.hasOwnProperty('returnValues')) {
      this.log.debug('\'returnValues\' option is deprecated in KindaDB');
    }
    if (!options.hasOwnProperty('properties')) {
      options.properties = '*';
    } else if (options.properties === '*') {
      // It's OK
    } else if (_.isArray(options.properties)) {
      // It's OK
    } else if (options.properties == null) {
      options.properties = [];
    } else {
      throw new Error('invalid \'properties\' option');
    }
    return options;
  };
});

module.exports = KindaDB;
