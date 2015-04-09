"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');
var util = require('kinda-util').create();

var Table = KindaObject.extend('Table', function() {
  this.setCreator(function(name, database) {
    if (!name) throw new Error('name is missing');
    this.name = name;
    this.database = database;
    this.indexes = [];
    this.isVirtual = true;
  });

  this.serialize = function() {
    if (this.isVirtual) return;
    return {
      name: this.name,
      indexes: this.indexes
    };
  };

  this.unserialize = function(json) {
    this.name = json.name;
    this.indexes = json.indexes;
    this.isVirtual = false;
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
        var keys = _.take(index.keys, queryKeys.length);
        return _.difference(queryKeys, keys).length === 0;
      });
    }
    var index = _.find(indexes, function(index) {
      var keys = _.drop(index.keys, queryKeys.length);
      return _.isEqual(keys, orderKeys);
    });
    if (!index) throw new Error('index not found');
    return index;
  };

  this.findIndexIndex = function(keys) {
    keys = this.normalizeKeys(keys);
    return _.findIndex(this.indexes, function(index) {
      return _.isEqual(index.keys, keys);
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
  }
});

module.exports = Table;
