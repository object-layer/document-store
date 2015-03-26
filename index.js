"use strict";

var _ = require('lodash');
var wait = require('co-wait');
var KindaObject = require('kinda-object');
var config = require('kinda-config').create();
var log = require('kinda-log').create();
var util = require('kinda-util').create();
var Store = require('kinda-store');

var VERSION = 1;

var KindaDB = KindaObject.extend('KindaDB', function() {
  this.Table = require('./table');

  this.setCreator(function(name, url, options) {
    if (!name) name = config['kinda-db'] && config['kinda-db'].name;
    if (!name) throw new Error('name is missing');
    if (!url) url = config['kinda-db'] && config['kinda-db'].url;
    if (!url) throw new Error('url is missing');
    if (!options) options = config['kinda-db'] && config['kinda-db'].options || {};
    this.name = name;
    this.store = Store.create(url);
    this.database = this;
    this._tables = [];
    this.migrations = [];
  });

  this.use = function(plugin) {
    plugin.plug(this);
  };

  // === Database ====

  this.registerMigration = function(number, fn) {
    if (!_.isNumber(number))
      throw new Error('invalid migration number');
    if (number < 1)
      throw new Error('migration number should be greater than 0');
    if (!_.isFunction(fn))
      throw new Error('migration function is missing');
    if (_.some(this.migrations, 'number', number))
      throw new Error('duplicated migration number');
    this.migrations.push({ number: number, fn: fn });
  };

  this.initializeDatabase = function *() {
    if (this.database._isInitialized) return;
    if (this._isInitializing) return;
    yield this.transaction(function *(tr) {
      try {
        tr._isInitializing = true;
        yield tr.createDatabase();
        yield tr.upgradeDatabase();
        yield tr.verifyDatabase();
        yield tr.migrateDatabase();
      } finally {
        tr._isInitializing = false;
      }
    }, { longTransaction: true, initializeDatabase: false });
    this.database._isInitialized = true;
    yield this.database.emitAsync('didInitialize');
  };

  this.createDatabase = function *() {
    yield this.store.transaction(function *(tr) {
      if (!(yield this.loadDatabase(tr, false))) {
        this.version = VERSION;
        this.lastMigrationNumber = 0;
        this.isLocked = false;
        yield this.saveDatabase(tr, true);
        log.info("Database '" + this.name + "' created");
      }
    }.bind(this));
  };

  this.lockDatabaseIf = function *(fn) {
    var done = false;
    while (!done) {
      yield this.store.transaction(function *(tr) {
        yield this.loadDatabase(tr);
        if (this.isLocked) return;
        if (fn(tr)) {
          this.isLocked = true;
          yield this.saveDatabase(tr);
        }
        done = true;
      }.bind(this));
      if (!done) {
        log.info("Waiting Database '" + this.name + "'...");
        yield wait(5000); // wait 5 secs before retrying
      }
    }
    return this.isLocked;
  };

  this.unlockDatabase = function *() {
    this.isLocked = false;
    yield this.saveDatabase(this.store);
  };

  this.upgradeDatabase = function *() {
    var upgradeIsNeeded = yield this.lockDatabaseIf(function() {
      return this.version !== VERSION;
    }.bind(this));
    if (!upgradeIsNeeded) return;

    try {
      this.emit('upgradeDidStart');

      // ... upgrading

      this.version = VERSION;
      log.info("Database '" + this.name + "' upgraded to version " + VERSION);
    } finally {
      yield this.unlockDatabase();
      this.emit('upgradeDidStop');
    }
  };

  this.verifyDatabase = function *() {
    // TODO: test isCreating and isDeleting flags to
    // detect incompletes indexes
  };

  this.migrateDatabase = function *(transaction) {
    if (!this.migrations.length) return;
    var maxMigrationNumber = _.max(this.migrations, 'number').number;

    var migrationIsNeeded = yield this.lockDatabaseIf(function() {
      if (this.lastMigrationNumber === maxMigrationNumber)
        return false;
      if (this.lastMigrationNumber > maxMigrationNumber)
        throw new Error('incompatible database (lastMigrationNumber > maxMigrationNumber)');
      return true;
    }.bind(this));
    if (!migrationIsNeeded) return;

    try {
      this.emit('migrationDidStart');
      var number = this.lastMigrationNumber;
      var migration;
      do {
        number++;
        migration = _.find(this.migrations, 'number', number);
        if (!migration) continue;
        yield migration.fn.call(this);
        log.info("Migration #" + number + " (database '" + this.name + "') done");
        this.lastMigrationNumber = number;
        yield this.saveDatabase(this.store);
      } while (number < maxMigrationNumber);
    } finally {
      yield this.unlockDatabase();
      this.emit('migrationDidStop');
    }
  };

  this.loadDatabase = function *(tr, errorIfMissing) {
    if (!tr) tr = this.store;
    if (errorIfMissing == null) errorIfMissing = true;
    var json = yield tr.get([this.name], { errorIfMissing: errorIfMissing });
    if (json) {
      this.unserialize(json);
      return true;
    }
  };

  this.saveDatabase = function *(tr, errorIfExists) {
    if (!tr) tr = this.store;
    var json = this.serialize();
    yield tr.put([this.name], json, {
      errorIfExists: errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.serialize = function() {
    return {
      version: this.version,
      name: this.name,
      lastMigrationNumber: this.lastMigrationNumber,
      isLocked: this.isLocked,
      tables: _.compact(_.invoke(this.getTables(), 'serialize'))
    };
  };

  this.unserialize = function(json) {
    this.version = json.version;
    this.name = json.name;
    this.lastMigrationNumber = json.lastMigrationNumber;
    this.isLocked = json.isLocked;
    json.tables.forEach(function(jsonTable) {
      var table = this.getTable(jsonTable.name);
      table.unserialize(jsonTable);
    }, this);
  };

  this.transaction = function *(fn, options) {
    if (this.database !== this)
      return yield fn(this); // we are already in a transaction
    if (!options) options = {};
    if (!options.hasOwnProperty('initializeDatabase'))
      options.initializeDatabase = true;
    if (options.initializeDatabase)
      yield this.initializeDatabase();
    if (options.longTransaction) {
      // For now, just use the regular store
      var transaction = Object.create(this);
      return yield fn(transaction);
    }
    return yield this.store.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.store = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.resetDatabase = function *() {
    this.database._isInitialized = false;
    yield this.store.delRange({ prefix: this.name });
    yield this.saveDatabase();
  };

  this.destroyDatabase = function *() {
    this.database._isInitialized = false;
    yield this.store.delRange({ prefix: this.name });
  };

  this.close = function *() {
    yield this.store.close();
  };

  // === Tables ====

  this.getTables = function() {
    return this._tables;
  };

  this.getTable = function(name) {
    var table = _.find(this.getTables(), 'name', name);
    if (!table) {
      table = this.Table.create(name, this.database);
      this.getTables().push(table);
    }
    return table;
  };

  this.initializeTable = function *(table) {
    yield this.initializeDatabase();
    if (table.isVirtual)
      throw new Error("Table '" + table.name + "' (database '" + this.name + "') is missing");
  };

  this.addTable = function *(name) {
    var table = this.getTable(name);
    if (!table.isVirtual)
      throw new Error("Table '" + name + "' (database '" + this.name + "') already exists");
    table.isVirtual = false;
    yield this.saveDatabase(this.store);
    log.info("Table '" + name + "' (database '" + this.name + "') created");
    return table;
  };

  this.normalizeTable = function(table) {
    if (_.isString(table))
      table = this.getTable(table);
    return table;
  };

  // === Indexes ====

  this.addIndex = function *(table, keys, options) {
    table = this.normalizeTable(table);
    keys = table.normalizeKeys(keys);
    if (!options) options = {};
    if (table.findIndexIndex(keys) !== -1)
      throw new Error('an index with the same keys already exists');
    var index = {
      name: keys.join('+'),
      keys: keys,
      isCreating: true // TODO: use this flag to detect incomplete index creation
    };
    if (options.projection != null) index.projection = options.projection;
    log.info("Creating index '" + index.name + "' (database '" + this.name + "', table '" + table.name + "')...");
    table.indexes.push(index);
    yield this.saveDatabase();
    yield this.forRange(table, {}, function *(item, key) {
      yield this.updateIndex(table, key, undefined, item, index);
    }, this);
    delete index.isCreating;
    yield this.saveDatabase();
  };

  this.delIndex = function *(table, keys, options) {
    table = this.normalizeTable(table);
    keys = table.normalizeKeys(keys);
    var i = table.findIndexIndex(keys);
    if (i === -1) throw new Error('index not found');
    var index = table.indexes[i];
    log.info("Deleting index '" + index.name + "' (database '" + this.name + "', table '" + table.name + "')...");
    index.isDeleting = true; // TODO: use this flag to detect incomplete index deletion
    yield this.saveDatabase();
    yield this.forRange(table, {}, function *(item, key) {
      // TODO: can be optimized with direct delete of the index records
      yield this.updateIndex(table, key, item, undefined, index);
    }, this);
    table.indexes.splice(i, 1);
    yield this.saveDatabase();
  };

  this.updateIndexes = function *(table, key, oldItem, newItem) {
    for (var i = 0; i < table.indexes.length; i++) {
      var index = table.indexes[i];
      yield this.updateIndex(table, key, oldItem, newItem, index);
    }
  };

  this.updateIndex = function *(table, key, oldItem, newItem, index) {
    oldItem = util.flattenObject(oldItem);
    newItem = util.flattenObject(newItem);
    var oldValues = [];
    var newValues = [];
    index.keys.forEach(function(key) {
      oldValues.push(oldItem[key]);
      newValues.push(newItem[key]);
    });
    var oldProjection;
    var newProjection;
    if (index.projection) {
      index.projection.forEach(function(key) {
        var val = oldItem[key];
        if (val != null) {
          if (!oldProjection) oldProjection = {};
          oldProjection[key] = val;
        }
        val = newItem[key];
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
    var indexKey = [this.name, this.makeIndexTableName(table, index)];
    indexKey.push.apply(indexKey, values);
    indexKey.push(key);
    return indexKey;
  };

  this.makeIndexTableName = function(table, index) {
    return table.name + ':' + index.name;
  };

  // === Basic operations ====

  // TODO: 'properties' option
  this.get = function *(table, key, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeGetOptions(options);
    var item = yield this.store.get(this.makeItemKey(table, key), options);
    return item;
  };

  this.put = function *(table, key, item, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    item = this.normalizeItem(item);
    options = this.normalizeOptions(options);
    yield this.transaction(function *(tr) {
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, { errorIfMissing: false });
      yield tr.store.put(itemKey, item, options);
      yield tr.updateIndexes(table, key, oldItem, item);
      yield tr.emitAsync('didPut', table, key, item, options);
    });
  };

  this.del = function *(table, key, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    key = this.normalizeKey(key);
    options = this.normalizeOptions(options);
    yield this.transaction(function *(tr) {
      var itemKey = tr.makeItemKey(table, key);
      var oldItem = yield tr.store.get(itemKey, options);
      if (oldItem) {
        yield tr.store.del(itemKey, options);
        yield tr.updateIndexes(table, key, oldItem, undefined);
        yield tr.emitAsync('didDel', table, key, oldItem, options);
      }
    });
  };

  // TODO: 'properties' option
  this.getMany = function *(table, keys, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    if (!_.isArray(keys))
      throw new Error('invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);
    options = this.normalizeGetOptions(options);
    var itemKeys = keys.map(function(key) {
      return this.makeItemKey(table, key)
    }, this);
    options = _.clone(options);
    options.returnValues =
      options.properties === '*' || options.properties.length;
    var items = yield this.store.getMany(itemKeys, options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  // TODO: 'properties' option
  this.getRange = function *(table, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    options = this.normalizeGetOptions(options);
    if (options.by)
      return yield this.getRangeBy(table, options.by, options);
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    options.returnValues =
      options.properties === '*' || options.properties.length;
    var items = yield this.store.getRange(options);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (options.returnValues) res.value = item.value;
      return res;
    });
    return items;
  };

  // TODO: 'properties' option
  this.getRangeBy = function *(table, index, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    index = table.normalizeIndex(index);
    options = this.normalizeGetOptions(options);
    var fetchItem = options.properties === '*';
    var useProjection = false;
    if (!fetchItem && options.properties.length) {
      var diff = _.difference(options.properties, index.projection);
      useProjection = diff.length === 0;
      if (!useProjection) {
        fetchItem = true;
        log.debug("an index projection doesn't satisfy requested properties, full item will be fetched");
      }
    }
    var queryOptions = _.clone(options);
    queryOptions.prefix = [this.name,
      this.makeIndexTableName(table, index)];
    if (options.prefix)
      queryOptions.prefix = queryOptions.prefix.concat(options.prefix);
    queryOptions.returnValues = useProjection;
    var items = yield this.store.getRange(queryOptions);
    items = items.map(function(item) {
      var res = { key: _.last(item.key) };
      if (useProjection) res.value = item.value;
      return res;
    });
    if (fetchItem) {
      var keys = items.map(function(item) { return item.key; });
      items = yield this.getMany(table, keys, { errorIfMissing: false });
    }
    return items;
  };

  this.getCount = function *(table, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    options = this.normalizeOptions(options);
    if (options.by)
      return yield this.getCountBy(table, options.by, options);
    options = _.clone(options);
    options.prefix = [this.name, table.name];
    return yield this.store.getCount(options);
  };

  this.getCountBy = function *(table, index, options) {
    table = this.normalizeTable(table);
    yield this.initializeTable(table);
    index = table.normalizeIndex(index);
    options = this.normalizeOptions(options);
    var queryOptions = _.clone(options);
    queryOptions.prefix = [this.name,
      this.makeIndexTableName(table, index)];
    if (options.prefix)
      queryOptions.prefix = queryOptions.prefix.concat(options.prefix);
    return yield this.store.getCount(queryOptions);
  };

  // === Advanced operations ===

  this.forRange = function *(table, options, fn, thisArg) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    options = _.clone(options);
    options.limit = 250;
    while (true) {
      var items = yield this.getRange(table, options);
      if (!items.length) break;
      for (var i = 0; i < items.length; i++) {
        var item = items[i];
        yield fn.call(thisArg, item.value, item.key);
      }
      var lastItem = _.last(items);
      options.startAfter = this.makeRangeKey(table, lastItem.key,
        lastItem.value, options);
      delete options.start;
      delete options.startBefore;
      delete options.value;
    };
  };

  this.delRange = function *(table, options) {
    table = this.normalizeTable(table);
    options = this.normalizeOptions(options);
    options = _.clone(options);
    options.returnValues = false;
    yield this.forRange(table, options, function *(item, key) {
      yield this.del(table, key, { errorIfMissing: false });
    }, this);
  };

  // === Helpers ====

  this.makeItemKey = function(table, key) {
    return [this.name, table.name, key];
  };

  this.makeRangeKey = function(table, key, item, options) {
    table = this.normalizeTable(table);
    key = this.normalizeKey(key);
    item = this.normalizeItem(item);
    options = this.normalizeOptions(options);
    if (!options.by) return [key];
    var index = table.normalizeIndex(options.by);
    var rangeKey = index.keys.map(function(k) {
      return item[k];
    });
    if (options.prefix) rangeKey.shift(); // TODO: support array prefixes
    rangeKey.push(key);
    return rangeKey;
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
    return options;
  };

  this.normalizeGetOptions = function(options) {
    options = this.normalizeOptions(options);
    if (options.hasOwnProperty('returnValues')) {
      log.debug("'returnValues' option is deprecated in KindaDBStore");
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
