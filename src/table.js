'use strict';

let _ = require('lodash');
let KindaObject = require('kinda-object');
let util = require('kinda-util').create();

let Table = KindaObject.extend('Table', function() {
  this.creator = function(options = {}) {
    if (!options.name) throw new Error('table name is missing');
    this.name = options.name;
    this.indexes = [];
    (options.indexes || []).forEach(index => {
      if (!_.isPlainObject(index)) index = { properties: index };
      this.addIndex(index);
    });
  };

  this.addIndex = function(options = {}) {
    let properties = this.normalizeIndexProperties(options.properties);
    let keys = _.pluck(properties, 'key');
    if (this.findIndexIndex(keys) !== -1) {
      throw new Error('an index with the same keys already exists');
    }
    let index = {
      name: keys.join('+'),
      properties
    };
    if (options.projection != null) index.projection = options.projection;
    this.indexes.push(index);
  };

  this.findIndex = function(keys) {
    keys = this.normalizeKeys(keys);
    let i = this.findIndexIndex(keys);
    if (i === -1) throw new Error('index not found');
    return this.indexes[i];
  };

  this.findIndexForQueryAndOrder = function(query, order) {
    if (!query) query = {};
    if (!order) order = [];
    order = this.normalizeKeys(order);
    let queryKeys = _.keys(query);
    let orderKeys = order;
    let indexes = this.indexes;
    if (queryKeys.length) {
      indexes = _.filter(indexes, idx => {
        let keys = _.pluck(idx.properties, 'key');
        keys = _.take(keys, queryKeys.length);
        return _.difference(queryKeys, keys).length === 0;
      });
    }
    let index = _.find(indexes, idx => {
      let keys = _.pluck(idx.properties, 'key');
      keys = _.drop(keys, queryKeys.length);
      return _.isEqual(keys, orderKeys);
    });
    if (!index) {
      throw new Error(`index not found (query=${JSON.stringify(query)}, order=${JSON.stringify(order)})`);
    }
    return index;
  };

  this.findIndexIndex = function(keys) {
    keys = this.normalizeKeys(keys);
    return _.findIndex(this.indexes, index => {
      let indexKeys = _.pluck(index.properties, 'key');
      return _.isEqual(indexKeys, keys);
    });
  };

  this.normalizeKeys = function(keys) {
    if (!_.isArray(keys)) keys = [keys];
    return keys;
  };

  this.normalizeIndex = function(indexOrKeys) {
    if (_.isString(indexOrKeys) || _.isArray(indexOrKeys)) {
      return this.findIndex(indexOrKeys);
    } else {
      return indexOrKeys;
    }
  };

  this.normalizeIndexProperties = function(properties) {
    if (!_.isArray(properties)) properties = [properties];
    properties = properties.map(property => {
      if (_.isString(property)) { // simple index
        return { key: property, value: true };
      } else if (_.isFunction(property)) { // computed index
        let key = util.getFunctionName(property);
        if (key === 'anonymous') throw new Error('invalid index definition: computed index function cannot be anonymous. Use a named function or set the displayName function property.');
        return { key, value: property };
      } else {
        throw new Error('invalid index definition');
      }
    });
    return properties;
  };
});

module.exports = Table;
