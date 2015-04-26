"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');
var util = require('kinda-util').create();

var Table = KindaObject.extend('Table', function() {
  this.setCreator(function(name, options) {
    if (!name) throw new Error('name is missing');
    if (!options) options = {};
    this.name = name;
    this.indexes = [];
    (options.indexes || []).forEach(function(index) {
      if (!_.isPlainObject(index)) index = { properties: index };
      var properties = index.properties;
      var options = _.omit(index, 'properties');
      this.addIndex(properties, options);
    }, this);
  });

  this.addIndex = function(properties, options) {
    var properties = this.normalizeIndexProperties(properties);
    if (!options) options = {};
    var keys = _.pluck(properties, 'key');
    if (this.findIndexIndex(keys) !== -1) {
      throw new Error('an index with the same keys already exists');
    }
    var index = {
      name: keys.join('+'),
      properties: properties
    };
    if (options.projection != null) index.projection = options.projection;
    this.indexes.push(index);
  };

  this.findIndex = function(keys) {
    keys = this.normalizeKeys(keys);
    var i = this.findIndexIndex(keys);
    if (i === -1) throw new Error('index not found');
    return this.indexes[i];
  };

  this.findIndexForQueryAndOrder = function(query, order) {
    if (!query) query = {};
    if (!order) order = [];
    order = this.normalizeKeys(order);
    var queryKeys = _.keys(query);
    var orderKeys = order;
    var indexes = this.indexes;
    if (queryKeys.length) {
      indexes = _.filter(indexes, function(index) {
        var keys = _.pluck(index.properties, 'key');
        keys = _.take(keys, queryKeys.length);
        return _.difference(queryKeys, keys).length === 0;
      });
    }
    var index = _.find(indexes, function(index) {
      var keys = _.pluck(index.properties, 'key');
      keys = _.drop(keys, queryKeys.length);
      return _.isEqual(keys, orderKeys);
    });
    if (!index) throw new Error('index not found');
    return index;
  };

  this.findIndexIndex = function(keys) {
    keys = this.normalizeKeys(keys);
    return _.findIndex(this.indexes, function(index) {
      var indexKeys = _.pluck(index.properties, 'key');
      return _.isEqual(indexKeys, keys);
    });
  };

  this.normalizeKeys = function(keys) {
    if (!_.isArray(keys)) keys = [keys];
    return keys;
  };

  this.normalizeIndex = function(indexOrKeys) {
    if (_.isString(indexOrKeys) || _.isArray(indexOrKeys))
      return this.findIndex(indexOrKeys);
    else
      return indexOrKeys;
  };

  this.normalizeIndexProperties = function(properties) {
    if (!_.isArray(properties)) properties = [properties];
    properties = properties.map(function(property) {
      if (_.isString(property)) { // simple index
        return { key: property, value: true };
      } else if (_.isFunction(property)) { // computed index
        var key = property.name || property.displayName;
        if (!key) throw new Error('invalid index definition: computed index function cannot be anonymous. Use a named function or set the displayName function property.');
        return { key: key, value: property };
      } else {
        throw new Error('invalid index definition');
      }
    });
    return properties;
  };
});

module.exports = Table;
