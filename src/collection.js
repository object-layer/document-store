'use strict';

import difference from 'lodash/difference';
import isEqual from 'lodash/isEqual';
import Index from './index-class';

export class Collection {
  constructor(options = {}) {
    if (typeof options === 'string') options = { name: options };
    if (!options.name) throw new Error('Collection name is missing');
    this.name = options.name;
    this.indexes = [];
    let indexes = options.indexes || [];
    indexes.forEach(index => {
      this.addIndex(index);
    });
  }

  addIndex(options = {}) {
    let index = new Index(options);
    if (this.findIndexIndex(index.keys) !== -1) {
      throw new Error('An index with the same keys already exists');
    }
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
        let keys = idx.keys.slice(0, queryKeys.length);
        return difference(queryKeys, keys).length === 0;
      });
    }
    let index = indexes.find(idx => {
      let keys = idx.keys.slice(queryKeys.length);
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
      return isEqual(index.keys, keys);
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

  toJSON() {
    return {
      name: this.name,
      indexes: this.indexes.map(index => index.toJSON())
    };
  }
}

export default Collection;
