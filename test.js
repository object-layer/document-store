'use strict';

import { assert } from 'chai';
import makeSortKey from 'make-sort-key';
import UniversalLog from 'universal-log';
import DocumentStore from './src';

let log = new UniversalLog();

describe('DocumentStore', function() {
  describe('migrations', function() {
    it('should handle one empty collection', async function() {
      let store, stats;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }],
          log
        });

        stats = await store.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 0);

        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 1);
      } finally {
        if (store) {
          await store.destroyAll();
          stats = await store.getStatistics();
          assert.strictEqual(stats.store.pairsCount, 0);
        }
      }
    });

    it('should handle one item in a collection', async function() {
      let store, stats;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }],
          log
        });
        await store.put('Collection1', 'aaa', { property1: 'value1' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 2);
      } finally {
        if (store) {
          await store.destroyAll();
        }
      }
    });

    it('should handle one collection added afterwards then removed', async function() {
      let store, stats;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 1);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }, { name: 'Collection2' }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 2);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection2' }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 1);
        assert.strictEqual(stats.removedCollectionsCount, 1);

        await store.removeCollectionsMarkedAsRemoved();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 1);
        assert.strictEqual(stats.removedCollectionsCount, 0);
      } finally {
        if (store) {
          await store.destroyAll();
        }
      }
    });

    it('should handle one index added afterwards then removed', async function() {
      let store, stats;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }],
          log
        });
        await store.put('Collection1', 'aaa', { property1: 'value1' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 2);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1', indexes: ['property1'] }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.store.pairsCount, 3);

        await store.put('Collection1', 'bbb', { property1: 'value2' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 5);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1' }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 3);
      } finally {
        if (store) {
          await store.destroyAll();
        }
      }
    });
  }); // migrations

  describe('simple document store', function() {
    let store;

    before(async function() {
      store = new DocumentStore({
        name: 'Test',
        url: 'mysql://test@localhost/test',
        collections: [{ name: 'Users' }],
        log
      });
    });

    after(async function() {
      await store.destroyAll();
    });

    it('should have a collections definition', async function() {
      assert.strictEqual(store.collections.length, 1);

      let collection = store.collections[0];
      assert.strictEqual(collection.name, 'Users');
      assert.strictEqual(collection.indexes.length, 0);
    });

    it('should put, get and delete some items', async function() {
      await store.put('Users', 'mvila', { firstName: 'Manu', age: 42 });
      let user = await store.get('Users', 'mvila');
      assert.deepEqual(user, { firstName: 'Manu', age: 42 });
      let hasBeenDeleted = await store.delete('Users', 'mvila');
      assert.isTrue(hasBeenDeleted);
      user = await store.get('Users', 'mvila', { errorIfMissing: false });
      assert.isUndefined(user);
      hasBeenDeleted = await store.delete('Users', 'mvila', { errorIfMissing: false });
      assert.isFalse(hasBeenDeleted);
    });
  }); // simple document store

  describe('rich document store', function() {
    let store;

    before(async function() {
      store = new DocumentStore({
        name: 'Test',
        url: 'mysql://test@localhost/test',
        collections: [
          {
            name: 'People',
            indexes: [
              ['lastName', 'firstName'],
              'age',
              ['country', 'city'],
              {
                properties: 'country',
                projection: ['firstName', 'lastName']
              },
              function fullNameSortKey(item) {
                return makeSortKey(item.lastName, item.firstName);
              }
            ]
          }
        ],
        log
      });
    });

    after(async function() {
      await store.destroyAll();
    });

    beforeEach(async function() {
      await store.put('People', 'aaa', {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
      await store.put('People', 'bbb', {
        firstName: 'Jack', lastName: 'Daniel',
        age: 60, city: 'New York', country: 'USA'
      });
      await store.put('People', 'ccc', {
        firstName: 'Bob', lastName: 'Cracker',
        age: 20, city: 'Los Angeles', country: 'USA'
      });
      await store.put('People', 'ddd', {
        firstName: 'Vincent', lastName: 'Vila',
        age: 43, city: 'Céret', country: 'France'
      });
      await store.put('People', 'eee', {
        firstName: 'Pierre', lastName: 'Dupont',
        age: 39, city: 'Lyon', country: 'France'
      });
      await store.put('People', 'fff', {
        firstName: 'Jacques', lastName: 'Fleur',
        age: 39, city: 'San Francisco', country: 'USA'
      });
    });

    afterEach(async function() {
      await store.delete('People', 'aaa', { errorIfMissing: false });
      await store.delete('People', 'bbb', { errorIfMissing: false });
      await store.delete('People', 'ccc', { errorIfMissing: false });
      await store.delete('People', 'ddd', { errorIfMissing: false });
      await store.delete('People', 'eee', { errorIfMissing: false });
      await store.delete('People', 'fff', { errorIfMissing: false });
    });

    it('should have a collections definition', async function() {
      assert.strictEqual(store.collections.length, 1);
      let collection = store.collections[0];
      assert.strictEqual(collection.name, 'People');
      assert.strictEqual(collection.indexes.length, 5);
      assert.strictEqual(collection.indexes[0].properties.length, 2);
      assert.strictEqual(collection.indexes[0].properties[0].key, 'lastName');
    });

    it('should get many items', async function() {
      let items = await store.getMany('People', ['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[1].key, 'ccc');
      assert.strictEqual(items[1].value.firstName, 'Bob');
    });

    it('should find all items in a collection', async function() {
      let items = await store.find('People');
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[5].key, 'fff');
      assert.strictEqual(items[5].value.firstName, 'Jacques');
    });

    it('should find and order items', async function() {
      let items = await store.find('People', { order: 'age' });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'ccc');
      assert.strictEqual(items[0].value.age, 20);
      assert.strictEqual(items[5].key, 'bbb');
      assert.strictEqual(items[5].value.age, 60);

      items = await store.find('People', { order: 'age', reverse: true });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'bbb');
      assert.strictEqual(items[0].value.age, 60);
      assert.strictEqual(items[5].key, 'ccc');
      assert.strictEqual(items[5].value.age, 20);

      let err = await catchError(async function() {
        await store.find('People', { order: 'missingProperty' });
      });
      assert.instanceOf(err, Error);
    });

    it('should find items with a query', async function() {
      let items = await store.find('People', { query: { country: 'France' } });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);

      items = await store.find('People', { query: { country: 'USA' } });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);

      items = await store.find('People', { query: { city: 'New York', country: 'USA' } });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['bbb']);

      items = await store.find('People', { query: { country: 'Japan' } });
      assert.strictEqual(items.length, 0);
    });

    it('should find items with a query and an order', async function() {
      let items = await store.find('People', {
        query: { country: 'USA' }, order: 'city'
      });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'fff']);

      items = await store.find('People', {
        query: { country: 'USA' }, order: 'city', reverse: true
      });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['fff', 'bbb', 'ccc']);
    });

    it('should find items after a specific item', async function() {
      let items = await store.find('People', {
        query: { country: 'USA' }, order: 'city', start: 'New York'
      });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'fff']);

      items = await store.find('People', {
        query: { country: 'USA' }, order: 'city', startAfter: 'New York'
      });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['fff']);
    });

    it('should find items before a specific item', async function() {
      let items = await store.find('People', {
        query: { country: 'USA' }, order: 'city', end: 'New York'
      });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb']);

      items = await store.find('People', {
        query: { country: 'USA' }, order: 'city', endBefore: 'New York'
      });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['ccc']);
    });

    it('should find a limited number of items', async function() {
      let items = await store.find('People', {
        query: { country: 'France' }, limit: 2
      });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd']);
    });

    it('should find items using an index projection', async function() {
      let items = await store.find('People', {
        query: { country: 'France' }, properties: ['firstName', 'lastName']
      });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, { firstName: 'Manuel', lastName: 'Vila' });

      items = await store.find('People', { // will not use projection
        query: { country: 'France' }, properties: ['firstName', 'lastName', 'age']
      });
      keys = pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
    });

    it('should find items using a computed index', async function() {
      let items = await store.find('People', { order: 'fullNameSortKey' });
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    it('should count all items in a collection', async function() {
      let count = await store.count('People');
      assert.strictEqual(count, 6);
    });

    it('should count items with a query', async function() {
      let count = await store.count('People', {
        query: { age: 39 }
      });
      assert.strictEqual(count, 2);

      count = await store.count('People', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      count = await store.count('People', {
        query: { country: 'France', city: 'Paris' }
      });
      assert.strictEqual(count, 1);

      count = await store.count('People', {
        query: { country: 'Japan', city: 'Tokyo' }
      });
      assert.strictEqual(count, 0);
    });

    it('should iterate over items', async function() {
      let keys = [];
      await store.forEach('People', { batchSize: 2 }, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    it('should iterate over items in a specific order', async function() {
      let keys = [];
      let options = { order: ['lastName', 'firstName'], batchSize: 2 };
      await store.forEach('People', options, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    it('should find and delete items', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = await store.findAndDelete('People', options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = await store.find('People');
      let keys = pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);
      deletedItemsCount = await store.findAndDelete('People', options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    it('shold change an item inside a transaction', async function() {
      assert.isFalse(store.insideTransaction);
      await store.transaction(async function(transaction) {
        assert.isTrue(transaction.insideTransaction);
        let innerItem = await transaction.get('People', 'aaa');
        assert.strictEqual(innerItem.firstName, 'Manuel');
        innerItem.firstName = 'Manu';
        await transaction.put('People', 'aaa', innerItem);
        innerItem = await transaction.get('People', 'aaa');
        assert.strictEqual(innerItem.firstName, 'Manu');
      });
      let item = await store.get('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manu');
    });

    it('should change an item inside an aborted transaction', async function() {
      try {
        assert.isFalse(store.insideTransaction);
        await store.transaction(async function(transaction) {
          assert.isTrue(transaction.insideTransaction);
          let innerItem = await transaction.get('People', 'aaa');
          assert.strictEqual(innerItem.firstName, 'Manuel');
          innerItem.firstName = 'Manu';
          await transaction.put('People', 'aaa', innerItem);
          innerItem = await transaction.get('People', 'aaa');
          assert.strictEqual(innerItem.firstName, 'Manu');
          throw new Error('something wrong');
        });
      } catch (err) {
        // noop
      }
      let item = await store.get('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manuel');
    });
  }); // rich document store
});

async function catchError(fn) {
  let err;
  try {
    await fn();
  } catch (e) {
    err = e;
  }
  return err;
}

function pluck(array, property) {
  return array.map(item => item[property]);
}
