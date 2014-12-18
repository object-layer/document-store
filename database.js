'use strict';

var _ = require('lodash');
var KindaObject = require('kinda-object');
var util = require('kinda-util').create();

var KindaDB = KindaObject.extend('KindaDB', function() {
  this.use = function(plugin) {
    plugin.plug(this);
  };

  this.getTable = function(name) {
    var table = _.find(this.tables, { name: name });
    if (!table) {
      table = this.Table.create(name, this.database);
      this.tables.push(table);
    }
    return table;
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

  this.normalizeTable = function(table) {
    if (_.isString(table))
      table = this.getTable(table);
    return table;
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
});

module.exports = KindaDB;
