"use strict";

var _ = require('lodash');
var wait = require('co-wait');
var KindaObject = require('kinda-object');
var config = require('kinda-config').get('kinda-db');
var log = require('kinda-log').create();
var util = require('kinda-util').create();
var Store = require('kinda-store');
var Table = require('./table');

var VERSION = 2;

var KindaDB = KindaObject.extend('KindaDB', function() {
  this.setCreator(function(name, url, tables, options) {
    if (!name) name = config.name;
    if (!name) throw new Error('name is missing');
    if (!url) url = config.url;
    if (!url) throw new Error('url is missing');
    if (!tables) tables = config.tables || [];
    if (!options) options = config.options || {};
    this.name = name;
    this.store = Store.create(url);
    this.database = this;
    this.tables = [];
    tables.forEach(function(table) {
      if (_.isString(table)) table = { name: table };
      var name = table.name;
      var options = _.omit(table, 'name');
      this.addTable(name, options);
    }, this);
  });

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
      var hasBeenCreated = yield this.createDatabaseIfDoesNotExist();
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
    return yield tr.get([this.name], { errorIfMissing: errorIfMissing });
  };

  this.saveDatabaseRecord = function *(tr, record, errorIfExists) {
    if (!tr) tr = this.store;
    yield tr.put([this.name], record, {
      errorIfExists: errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createDatabaseIfDoesNotExist = function *() {
    var hasBeenCreated = false;
    yield this.store.transaction(function *(tr) {
      var record = yield this.loadDatabaseRecord(tr, false);
      if (!record) {
        var tables = this.tables.map(function(table) {
          return {
            name: table.name,
            indexes: _.pluck(table.indexes, 'name')
          }
        });
        record = {
          name: this.name,
          version: VERSION,
          tables: tables
        };
        yield this.saveDatabaseRecord(tr, record, true);
        hasBeenCreated = true;
        yield this.emitAsync('didCreate', tr);
        log.info("Database '" + this.name + "' created");
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.lockDatabase = function *() {
    var hasBeenLocked = false;
    while (!hasBeenLocked) {
      yield this.store.transaction(function *(tr) {
        var record = yield this.loadDatabaseRecord(tr);
        if (!record.isLocked) {
          record.isLocked = hasBeenLocked = true;
          yield this.saveDatabaseRecord(tr, record);
        }
      }.bind(this));
      if (!hasBeenLocked) {
        log.info("Waiting Database '" + this.name + "'...");
        yield wait(5000); // wait 5 secs before retrying
      }
    }
  };

  this.unlockDatabase = function *() {
    var record = yield this.loadDatabaseRecord();
    record.isLocked = false;
    yield this.saveDatabaseRecord(undefined, record);
  };

  this.upgradeDatabase = function *() {
    var record = yield this.loadDatabaseRecord();
    var version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('cannot downgrade the database');
    }

    this.emit('upgradeDidStart');

    if (version < 2) {
      delete record.lastMigrationNumber;
      record.tables.forEach(function(table) {
        table.indexes = _.pluck(table.indexes, 'name');
      });
    }

    record.version = VERSION;
    yield this.saveDatabaseRecord(undefined, record);
    log.info("Database '" + this.name + "' upgraded to version " + VERSION);

    this.emit('upgradeDidStop');
  };

  this.verifyDatabase = function *() {
    // ...
  };

  this.migrateDatabase = function *(transaction) {
    var record = yield this.loadDatabaseRecord();
    try {
      // Find out added or updated tables
      for (var i = 0; i < this.tables.length; i++) {
        var table = this.tables[i];
        var existingTable = _.find(record.tables, 'name', table.name);
        if (!existingTable) {
          this._emitMigrationDidStart();
          record.tables.push({
            name: table.name,
            indexes: _.pluck(table.indexes, 'name')
          });
          yield this.saveDatabaseRecord(undefined, record);
          log.info("Table '" + table.name + "' (database '" + this.name + "') added");
        } else if (existingTable.hasBeenRemoved) {
          throw new Error('adding a table that has been removed is not implemented yet');
        } else {
          // Find out added indexes
          for (var j = 0; j < table.indexes.length; j++) {
            var index = table.indexes[j];
            if (!_.contains(existingTable.indexes, index.name)) {
              this._emitMigrationDidStart();
              yield this._addIndex(table, index);
              existingTable.indexes.push(index.name);
              yield this.saveDatabaseRecord(undefined, record);
            }
          }
          // Find out removed indexes
          var existingIndexNames = _.clone(existingTable.indexes);
          for (var j = 0; j < existingIndexNames.length; j++) {
            var existingIndexName = existingIndexNames[j];
            if (!_.find(table.indexes, 'name', existingIndexName)) {
              this._emitMigrationDidStart();
              yield this._removeIndex(table.name, existingIndexName);
              _.pull(existingTable.indexes, existingIndexName);
              yield this.saveDatabaseRecord(undefined, record);
            }
          }
        }
      }

      // Find out removed tables
      for (var i = 0; i < record.tables.length; i++) {
        var existingTable = record.tables[i];
        if (existingTable.hasBeenRemoved) continue;
        var table =  _.find(this.tables, 'name', existingTable.name);
        if (!table) {
          this._emitMigrationDidStart();
          for (var j = 0; j < existingTable.indexes.length; j++) {
            var existingIndexName = existingTable.indexes[j];
            yield this._removeIndex(existingTable.name, existingIndexName);
          }
          existingTable.indexes.length = 0;
          existingTable.hasBeenRemoved = true;
          yield this.saveDatabaseRecord(undefined, record);
          log.info("Table '" + existingTable.name + "' (database '" + this.name + "') marked as removed");
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
    log.info("Adding index '" + index.name + "' (database '" + this.name + "', table '" + table.name + "')...");
    yield this.forEachItems(table, {}, function *(item, key) {
      yield this.updateIndex(table, key, undefined, item, index);
    }, this);
  };

  this._removeIndex = function *(tableName, indexName) {
    log.info("Removing index '" + indexName + "' (database '" + this.name + "', table '" + tableName + "')...");
    var prefix = [this.name, this.makeIndexTableName(tableName, indexName)];
    yield this.store.delRange({ prefix: prefix });
  };

  this._removeTable = function *(tableName) { // used by kinda-object-db
    var prefix = [this.name, tableName];
    yield this.store.delRange({ prefix: prefix });
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction()) return yield fn(this);
    yield this.initializeDatabase();
    return yield this.store.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.store = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.isInsideTransaction = function() {
    return this !== this.database;
  };

  this.getStatistics = function *() {
    var tablesCount = 0;
    var removedTablesCount = 0;
    var indexesCount = 0;
    var record = yield this.loadDatabaseRecord(undefined, false);
    if (record) {
      record.tables.forEach(function(table) {
        if (!table.hasBeenRemoved) {
          tablesCount++;
        } else {
          removedTablesCount++;
        }
        indexesCount += table.indexes.length;
      });
    }
    var storePairsCount = yield this.store.getCount({ prefix: this.name });
    return {
      tablesCount: tablesCount,
      removedTablesCount: removedTablesCount,
      indexesCount: indexesCount,
      store: {
        pairsCount: storePairsCount
      }
    };
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
    var table = _.find(this.tables, 'name', name);
    if (!table && errorIfMissing) {
      throw new Error("Table '" + table.name + "' (database '" + this.name + "') is missing");
    }
    return table;
  };

  this.addTable = function(name, options) {
    var table = this.getTable(name, false);
    if (table) {
      throw new Error("Table '" + name + "' (database '" + this.name + "') already exists");
    }
    table = Table.create(name, options);
    this.tables.push(table);
  };

  this.normalizeTable = function(table) {
    if (_.isString(table)) table = this.getTable(table);
    return table;
  };

  // === Indexes ====

  this.updateIndexes = function *(table, key, oldItem, newItem) {
    for (var i = 0; i < table.indexes.length; i++) {
      var index = table.indexes[i];
      yield this.updateIndex(table, key, oldItem, newItem, index);
    }
  };

  this.updateIndex = function *(table, key, oldItem, newItem, index) {
    var flattenedOldItem = util.flattenObject(oldItem);
    var flattenedNewItem = util.flattenObject(newItem);
    var oldValues = [];
    var newValues = [];
    index.properties.forEach(function(property) {
      var oldValue, newValue;
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
    var oldProjection;
    var newProjection;
    if (index.projection) {
      index.projection.forEach(function(key) {
        var val = flattenedOldItem[key];
        if (val != null) {
          if (!oldProjection) oldProjection = {};
          oldProjection[key] = val;
        }
        val = flattenedNewItem[key];
        if (val != null) {
          if (!newProjection) newProjection = {};
          newProjection[key] = val;
        }
      }, this);
    };
    var valuesAreDifferent = !_.isEqual(oldValues, newValues);
    var projectionIsDifferent = !_.isEqual(oldProjection, newProjection);
    if (valuesAreDifferent && !_.contains(oldValues, undefined)) {
      var indexKey = this.makeIndexKey(table, index, oldValues, key);
      yield this.store.del(indexKey);
    };
    if ((valuesAreDifferent || projectionIsDifferent)
      && !_.contains(newValues, undefined)) {
      var indexKey = this.makeIndexKey(table, index, newValues, key);
      yield this.store.put(indexKey, newProjection);
    };
  };

  this.makeIndexKey = function(table, index, values, key) {
    var indexKey = [this.name, this.makeIndexTableName(table.name, index.name)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  };

  this.makeIndexTableName = function(tableName, indexName) {
    return tableName + ':' + indexName;
  };

  this.makeIndexKeyForQuery = function(table, index, query) {
    if (!query) query = {};
    var indexKey = [this.name, this.makeIndexTableName(table.name, index.name)];
    var keys = _.pluck(index.properties, 'key');
    var queryKeys = _.keys(query);
    for (var i = 0; i < queryKeys.length; i++) {
      var key = keys[i];
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
    var item = yield this.store.get(this.makeItemKey(table, key), options);
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
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, { errorIfMissing: false });
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
    yield this.initializeDatabase();
    yield this.transaction(function *(tr) {
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, options);
      if (oldItem) {
        yield tr.store.del(itemKey, options);
        yield tr.updateIndexes(table, key, oldItem, undefined);
        yield tr.emitAsync('didDeleteItem', table, key, oldItem, options);
      }
    });
  };

  // Options:
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. Default: '*'. TODO
  this.getItems = function *(table, keys, options) {
    table = this.normalizeTable(table);
    if (!_.isArray(keys))
      throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeOptions(options);
    var itemKeys = keys.map(function(key) {
      return this.makeItemKey(table, key)
    }, this);
    options = _.clone(options);
    options.returnValues = options.properties === '*' || options.properties.length;
    yield this.initializeDatabase();
    var items = yield this.store.getMany(itemKeys, options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
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
    if (!_.isEmpty(options.query) || !_.isEmpty(options.order))
      return yield this._findItemsWithIndex(table, options);
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    options.returnValues = options.properties === '*' || options.properties.length;
    yield this.initializeDatabase();
    var items = yield this.store.getRange(options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  this._findItemsWithIndex = function *(table, options) {
    var index = table.findIndexForQueryAndOrder(options.query, options.order);

    var fetchItem = options.properties === '*';
    var useProjection = false;
    if (!fetchItem && options.properties.length) {
      var diff = _.difference(options.properties, index.projection);
      useProjection = diff.length === 0;
      if (!useProjection) {
        fetchItem = true;
        log.debug('an index projection doesn\'t satisfy requested properties, full item will be fetched');
      }
    }

    options = _.clone(options);
    options.prefix = this.makeIndexKeyForQuery(table, index, options.query);
    options.returnValues = useProjection;

    yield this.initializeDatabase();
    var items = yield this.store.getRange(options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (useProjection) res.value = item.value;
      return res;
    });

    if (fetchItem) {
      var keys = _.pluck(items, 'key');
      items = yield this.getItems(table, keys, { errorIfMissing: false });
    }

    return items;
  };

  // Options: same as findItems() without 'reverse' and 'properties' attributes.
  this.countItems = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    if (!_.isEmpty(options.query) || !_.isEmpty(options.order))
      return yield this._countItemsWithIndex(table, options);
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    yield this.initializeDatabase();
    return yield this.store.getCount(options);
  };

  this._countItemsWithIndex = function *(table, options) {
    var index = table.findIndexForQueryAndOrder(options.query, options.order);
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
      var items = yield this.findItems(table, options);
      if (!items.length) break;
      for (var i = 0; i < items.length; i++) {
        var item = items[i];
        yield fn.call(thisArg, item.value, item.key);
      }
      var lastItem = _.last(items);
      options.startAfter = this.makeOrderKey(table, lastItem.key, lastItem.value, options.order);
      delete options.start;
    };
  };

  // Options: same as forEachItems() without 'properties' attribute.
  this.findAndDeleteItems = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    options = _.clone(options);
    options.properties = [];
    yield this.forEachItems(table, options, function *(value, key) {
      yield this.deleteItem(table, key, { errorIfMissing: false });
    }, this);
  };

  // === Helpers ====

  this.makeItemKey = function(table, key) {
    return [this.name, table.name, key];
  };

  this.makeOrderKey = function(table, key, value, order) {
    if (!order) order = [];
    var orderKey = [];
    order.forEach(function(key) {
      orderKey.push(value[key]);
    }, this);
    orderKey.push(key);
    return orderKey;
  };

  this.normalizeKey = function(key) {
    if (typeof key !== 'number' && typeof key !== 'string')
      throw new Error('invalid key type');
    if (!key)
      throw new Error('key is null or empty');
    return key;
  };

  this.normalizeItem = function(item) {
    if (!_.isObject(item)) {
      throw new Error('invalid item type');
    };
    return item;
  };

  this.normalizeOptions = function(options) {
    if (!options) options = {};
    if (options.hasOwnProperty('returnValues')) {
      log.debug("'returnValues' option is deprecated in KindaDB");
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
      throw new Error("invalid 'properties' option");
    }
    return options;
  };
});

module.exports = KindaDB;
