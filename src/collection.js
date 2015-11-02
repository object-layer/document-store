'use strict';

import fnName from 'fn-name';
import isPlainObject from 'lodash.isplainobject';
import difference from 'lodash.difference';
import isEqual from 'lodash.isequal';

export class Collection {
  constructor(options = {}) {
    if (!options.name) throw new Error('Collection name is missing');
    this.name = options.name;
    this.indexes = [];
    (options.indexes || []).forEach(index => {
      if (!isPlainObject(index)) index = { properties: index };
      this.addIndex(index);
    });
  }

  addIndex(options = {}) {
    let properties = this.normalizeIndexProperties(options.properties);
    let keys = properties.map(property => property.key);
    if (this.findIndexIndex(keys) !== -1) {
      throw new Error('An index with the same keys already exists');
    }
    let index = {
      name: keys.join('+'),
      properties
    };
    if (options.projection != null) index.projection = options.projection;
    this.indexes.push(index);
  }

  findIndex(keys) {
    keys = this.normalizeKeys(keys);
    let i = this.findIndexIndex(keys);
    if (i === -1) throw new Error('Index not found');
    return this.indexes[i];
  }

  findIndexForQueryAndOrder(query, order) {
    if (!query) query = {};
    if (!order) order = [];
    order = this.normalizeKeys(order);
    let queryKeys = Object.keys(query);
    let orderKeys = order;
    let indexes = this.indexes;
    if (queryKeys.length) {
      indexes = indexes.filter(idx => {
        let keys = idx.properties.map(property => property.key);
        keys = keys.slice(0, queryKeys.length);
        return difference(queryKeys, keys).length === 0;
      });
    }
    let index = indexes.find(idx => {
      let keys = idx.properties.map(property => property.key);
      keys = keys.slice(queryKeys.length);
      return isEqual(keys, orderKeys);
    });
    if (!index) {
      throw new Error(`Index not found (query=${JSON.stringify(query)}, order=${JSON.stringify(order)})`);
    }
    return index;
  }

  findIndexIndex(keys) {
    keys = this.normalizeKeys(keys);
    return this.indexes.findIndex(index => {
      let indexKeys = index.properties.map(property => property.key);
      return isEqual(indexKeys, keys);
    });
  }

  normalizeKeys(keys) {
    if (!Array.isArray(keys)) keys = [keys];
    return keys;
  }

  normalizeIndex(indexOrKeys) {
    if (typeof indexOrKeys === 'string' || Array.isArray(indexOrKeys)) {
      return this.findIndex(indexOrKeys);
    } else {
      return indexOrKeys;
    }
  }

  normalizeIndexProperties(properties) {
    if (!Array.isArray(properties)) properties = [properties];
    properties = properties.map(property => {
      if (typeof property === 'string') { // simple index
        return { key: property, value: true };
      } else if (typeof property === 'function') { // computed index
        let key = fnName(property);
        if (!key) throw new Error('Invalid index definition: computed index function cannot be anonymous. Use a named function or set the displayName function property.');
        return { key, value: property };
      } else {
        throw new Error('Invalid index definition');
      }
    });
    return properties;
  }
}

export default Collection;
