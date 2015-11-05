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
          collections: ['Collection1'],
          log
        });

        stats = await store.getStatistics();
        assert.strictEqual(stats.keyValueStore.pairsCount, 0);

        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.keyValueStore.pairsCount, 1);
      } finally {
        if (store) {
          await store.destroyAll();
          stats = await store.getStatistics();
          assert.strictEqual(stats.keyValueStore.pairsCount, 0);
        }
      }
    });

    it('should handle one document in a collection', async function() {
      let store, stats;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: ['Collection1'],
          log
        });
        await store.put('Collection1', 'aaa', { property1: 'value1' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.keyValueStore.pairsCount, 2);
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
          collections: ['Collection1'],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 1);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: ['Collection1', 'Collection2'],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.collectionsCount, 2);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: ['Collection2'],
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
          collections: ['Collection1'],
          log
        });
        await store.put('Collection1', 'aaa', { property1: 'value1' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.keyValueStore.pairsCount, 2);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{ name: 'Collection1', indexes: ['property1'] }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.keyValueStore.pairsCount, 3);

        await store.put('Collection1', 'bbb', { property1: 'value2' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.keyValueStore.pairsCount, 5);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: ['Collection1'],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.keyValueStore.pairsCount, 3);
      } finally {
        if (store) {
          await store.destroyAll();
        }
      }
    });

    it('should rebuild updated indexes', async function() {
      let store, stats, people;
      try {
        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{
            name: 'People',
            indexes: ['name']
          }],
          log
        });
        await store.put('People', 'aaa', { name: 'Manu' });
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.keyValueStore.pairsCount, 3);
        people = await store.find('People', { query: { name: 'Manu' } });
        assert.deepEqual(people, [ { key: 'aaa', value: { name: 'Manu' } } ]);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{
            name: 'People',
            indexes: [{
              properties: 'name',
              projection: ['name']
            }]
          }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.keyValueStore.pairsCount, 3);
        people = await store.find('People', { query: { name: 'Manu' }, properties: ['name'] });
        assert.deepEqual(people, [ { key: 'aaa', value: { name: 'Manu' } } ]);

        store = new DocumentStore({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          collections: [{
            name: 'People',
            indexes: [{
              properties: function name(doc) {
                return doc.name && doc.name.toLowerCase();
              },
              projection: ['name'],
              version: 2
            }]
          }],
          log
        });
        await store.initializeDocumentStore();
        stats = await store.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.keyValueStore.pairsCount, 3);
        people = await store.find('People', { query: { name: 'manu' }, properties: ['name'] });
        assert.deepEqual(people, [ { key: 'aaa', value: { name: 'Manu' } } ]);
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

    it('should put, get and delete some documents', async function() {
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
              function fullNameSortKey(doc) {
                return makeSortKey(doc.lastName, doc.firstName);
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

    it('should get many documents', async function() {
      let docs = await store.getMany('People', ['aaa', 'ccc']);
      assert.strictEqual(docs.length, 2);
      assert.strictEqual(docs[0].key, 'aaa');
      assert.strictEqual(docs[0].value.firstName, 'Manuel');
      assert.strictEqual(docs[1].key, 'ccc');
      assert.strictEqual(docs[1].value.firstName, 'Bob');
    });

    it('should find all documents in a collection', async function() {
      let docs = await store.find('People');
      assert.strictEqual(docs.length, 6);
      assert.strictEqual(docs[0].key, 'aaa');
      assert.strictEqual(docs[0].value.firstName, 'Manuel');
      assert.strictEqual(docs[5].key, 'fff');
      assert.strictEqual(docs[5].value.firstName, 'Jacques');
    });

    it('should find and order documents', async function() {
      let docs = await store.find('People', { order: 'age' });
      assert.strictEqual(docs.length, 6);
      assert.strictEqual(docs[0].key, 'ccc');
      assert.strictEqual(docs[0].value.age, 20);
      assert.strictEqual(docs[5].key, 'bbb');
      assert.strictEqual(docs[5].value.age, 60);

      docs = await store.find('People', { order: 'age', reverse: true });
      assert.strictEqual(docs.length, 6);
      assert.strictEqual(docs[0].key, 'bbb');
      assert.strictEqual(docs[0].value.age, 60);
      assert.strictEqual(docs[5].key, 'ccc');
      assert.strictEqual(docs[5].value.age, 20);

      let err = await catchError(async function() {
        await store.find('People', { order: 'missingProperty' });
      });
      assert.instanceOf(err, Error);
    });

    it('should find documents with a query', async function() {
      let docs = await store.find('People', { query: { country: 'France' } });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);

      docs = await store.find('People', { query: { country: 'USA' } });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);

      docs = await store.find('People', { query: { city: 'New York', country: 'USA' } });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['bbb']);

      docs = await store.find('People', { query: { country: 'Japan' } });
      assert.strictEqual(docs.length, 0);
    });

    it('should find documents with a query and an order', async function() {
      let docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city'
      });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'fff']);

      docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city', reverse: true
      });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['fff', 'bbb', 'ccc']);
    });

    it('should find documents after a specific document', async function() {
      let docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city', start: 'New York'
      });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['bbb', 'fff']);

      docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city', startAfter: 'New York'
      });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['fff']);
    });

    it('should find documents before a specific document', async function() {
      let docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city', end: 'New York'
      });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb']);

      docs = await store.find('People', {
        query: { country: 'USA' }, order: 'city', endBefore: 'New York'
      });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['ccc']);
    });

    it('should find a limited number of documents', async function() {
      let docs = await store.find('People', {
        query: { country: 'France' }, limit: 2
      });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd']);
    });

    it('should find documents using an index projection', async function() {
      let docs = await store.find('People', {
        query: { country: 'France' }, properties: ['firstName', 'lastName']
      });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(docs[0].value, { firstName: 'Manuel', lastName: 'Vila' });

      docs = await store.find('People', { // will not use projection
        query: { country: 'France' }, properties: ['firstName', 'lastName', 'age']
      });
      keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(docs[0].value, {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
    });

    it('should find documents using a computed index', async function() {
      let docs = await store.find('People', { order: 'fullNameSortKey' });
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    it('should count all documents in a collection', async function() {
      let count = await store.count('People');
      assert.strictEqual(count, 6);
    });

    it('should count documents with a query', async function() {
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

    it('should iterate over documents', async function() {
      let keys = [];
      await store.forEach('People', { batchSize: 2 }, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    it('should iterate over documents in a specific order', async function() {
      let keys = [];
      let options = { order: ['lastName', 'firstName'], batchSize: 2 };
      await store.forEach('People', options, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    it('should find and delete documents', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedDocsCount = await store.findAndDelete('People', options);
      assert.strictEqual(deletedDocsCount, 3);
      let docs = await store.find('People');
      let keys = pluck(docs, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);
      deletedDocsCount = await store.findAndDelete('People', options);
      assert.strictEqual(deletedDocsCount, 0);
    });

    it('shold change a document inside a transaction', async function() {
      assert.isFalse(store.insideTransaction);
      await store.transaction(async function(transaction) {
        assert.isTrue(transaction.insideTransaction);
        let innerDoc = await transaction.get('People', 'aaa');
        assert.strictEqual(innerDoc.firstName, 'Manuel');
        innerDoc.firstName = 'Manu';
        await transaction.put('People', 'aaa', innerDoc);
        innerDoc = await transaction.get('People', 'aaa');
        assert.strictEqual(innerDoc.firstName, 'Manu');
      });
      let doc = await store.get('People', 'aaa');
      assert.strictEqual(doc.firstName, 'Manu');
    });

    it('should change a document inside an aborted transaction', async function() {
      try {
        assert.isFalse(store.insideTransaction);
        await store.transaction(async function(transaction) {
          assert.isTrue(transaction.insideTransaction);
          let innerDoc = await transaction.get('People', 'aaa');
          assert.strictEqual(innerDoc.firstName, 'Manuel');
          innerDoc.firstName = 'Manu';
          await transaction.put('People', 'aaa', innerDoc);
          innerDoc = await transaction.get('People', 'aaa');
          assert.strictEqual(innerDoc.firstName, 'Manu');
          throw new Error('something wrong');
        });
      } catch (err) {
        // noop
      }
      let doc = await store.get('People', 'aaa');
      assert.strictEqual(doc.firstName, 'Manuel');
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
