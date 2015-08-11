'use strict';

let assert = require('chai').assert;
let _ = require('lodash');
let util = require('kinda-util').create();
let KindaDB = require('./src');

suite('KindaDB', function() {
  let catchError = async function(fn) {
    let err;
    try {
      await fn();
    } catch (e) {
      err = e;
    }
    return err;
  };

  suite('migrations', function() {
    test('one empty table', async function() {
      let db, stats;
      try {
        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }]
        });

        stats = await db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 0);

        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 1);
      } finally {
        await db.destroyDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 0);
      }
    });

    test('one item in a table', async function() {
      let db, stats;
      try {
        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }]
        });
        await db.putItem('Table1', 'aaa', { property1: 'value1' });
        stats = await db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 2);
      } finally {
        await db.destroyDatabase();
      }
    });

    test('one table added afterwards then removed', async function() {
      let db, stats;
      try {
        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }]
        });
        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);

        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }, { name: 'Table2' }]
        });
        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.tablesCount, 2);

        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table2' }]
        });
        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);
        assert.strictEqual(stats.removedTablesCount, 1);

        await db.removeTablesMarkedAsRemoved();
        stats = await db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);
        assert.strictEqual(stats.removedTablesCount, 0);
      } finally {
        await db.destroyDatabase();
      }
    });

    test('one index added afterwards then removed', async function() {
      let db, stats;
      try {
        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }]
        });
        await db.putItem('Table1', 'aaa', { property1: 'value1' });
        stats = await db.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 2);

        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1', indexes: ['property1'] }]
        });
        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.store.pairsCount, 3);

        await db.putItem('Table1', 'bbb', { property1: 'value2' });
        stats = await db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 5);

        db = KindaDB.create({
          name: 'Test',
          url: 'mysql://test@localhost/test',
          tables: [{ name: 'Table1' }]
        });
        await db.initializeDatabase();
        stats = await db.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 3);
      } finally {
        await db.destroyDatabase();
      }
    });
  }); // migrations suite

  suite('simple database', function() {
    let db;

    suiteSetup(async function() {
      db = KindaDB.create({
        name: 'Test',
        url: 'mysql://test@localhost/test',
        tables: [{ name: 'Users' }]
      });
    });

    suiteTeardown(async function() {
      await db.destroyDatabase();
    });

    test('tables definition', async function() {
      assert.strictEqual(db.tables.length, 1);

      let table = db.tables[0];
      assert.strictEqual(table.name, 'Users');
      assert.strictEqual(table.indexes.length, 0);
    });

    test('put, get and delete some items', async function() {
      await db.putItem('Users', 'mvila', { firstName: 'Manu', age: 42 });
      let user = await db.getItem('Users', 'mvila');
      assert.deepEqual(user, { firstName: 'Manu', age: 42 });
      let hasBeenDeleted = await db.deleteItem('Users', 'mvila');
      assert.isTrue(hasBeenDeleted);
      user = await db.getItem('Users', 'mvila', { errorIfMissing: false });
      assert.isUndefined(user);
      hasBeenDeleted = await db.deleteItem('Users', 'mvila', { errorIfMissing: false });
      assert.isFalse(hasBeenDeleted);
    });
  }); // simple database suite

  suite('rich database', function() {
    let db;

    suiteSetup(async function() {
      db = KindaDB.create({
        name: 'Test',
        url: 'mysql://test@localhost/test',
        tables: [
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
                return util.makeSortKey(item.lastName, item.firstName);
              }
            ]
          }
        ]
      });
    });

    suiteTeardown(async function() {
      await db.destroyDatabase();
    });

    setup(async function() {
      await db.putItem('People', 'aaa', {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
      await db.putItem('People', 'bbb', {
        firstName: 'Jack', lastName: 'Daniel',
        age: 60, city: 'New York', country: 'USA'
      });
      await db.putItem('People', 'ccc', {
        firstName: 'Bob', lastName: 'Cracker',
        age: 20, city: 'Los Angeles', country: 'USA'
      });
      await db.putItem('People', 'ddd', {
        firstName: 'Vincent', lastName: 'Vila',
        age: 43, city: 'Céret', country: 'France'
      });
      await db.putItem('People', 'eee', {
        firstName: 'Pierre', lastName: 'Dupont',
        age: 39, city: 'Lyon', country: 'France'
      });
      await db.putItem('People', 'fff', {
        firstName: 'Jacques', lastName: 'Fleur',
        age: 39, city: 'San Francisco', country: 'USA'
      });
    });

    teardown(async function() {
      await db.deleteItem('People', 'aaa', { errorIfMissing: false });
      await db.deleteItem('People', 'bbb', { errorIfMissing: false });
      await db.deleteItem('People', 'ccc', { errorIfMissing: false });
      await db.deleteItem('People', 'ddd', { errorIfMissing: false });
      await db.deleteItem('People', 'eee', { errorIfMissing: false });
      await db.deleteItem('People', 'fff', { errorIfMissing: false });
    });

    test('tables definition', async function() {
      assert.strictEqual(db.tables.length, 1);
      let table = db.tables[0];
      assert.strictEqual(table.name, 'People');
      assert.strictEqual(table.indexes.length, 5);
      assert.strictEqual(table.indexes[0].properties.length, 2);
      assert.strictEqual(table.indexes[0].properties[0].key, 'lastName');
    });

    test('get many items', async function() {
      let items = await db.getItems('People', ['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[1].key, 'ccc');
      assert.strictEqual(items[1].value.firstName, 'Bob');
    });

    test('find all items in a table', async function() {
      let items = await db.findItems('People');
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[5].key, 'fff');
      assert.strictEqual(items[5].value.firstName, 'Jacques');
    });

    test('find and order items', async function() {
      let items = await db.findItems('People', { order: 'age' });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'ccc');
      assert.strictEqual(items[0].value.age, 20);
      assert.strictEqual(items[5].key, 'bbb');
      assert.strictEqual(items[5].value.age, 60);

      items = await db.findItems('People', { order: 'age', reverse: true });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'bbb');
      assert.strictEqual(items[0].value.age, 60);
      assert.strictEqual(items[5].key, 'ccc');
      assert.strictEqual(items[5].value.age, 20);

      let err = await catchError(async function() {
        await db.findItems('People', { order: 'missingProperty' });
      });
      assert.instanceOf(err, Error);
    });

    test('find items with a query', async function() {
      let items = await db.findItems('People', { query: { country: 'France' } });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);

      items = await db.findItems('People', { query: { country: 'USA' } });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);

      items = await db.findItems('People', { query: { city: 'New York', country: 'USA' } });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb']);

      items = await db.findItems('People', { query: { country: 'Japan' } });
      assert.strictEqual(items.length, 0);
    });

    test('find items with a query and an order', async function() {
      let items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city'
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'fff']);

      items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city', reverse: true
      });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['fff', 'bbb', 'ccc']);
    });

    test('find items after a specific item', async function() {
      let items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city', start: 'New York'
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'fff']);

      items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city', startAfter: 'New York'
      });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['fff']);
    });

    test('find items before a specific item', async function() {
      let items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city', end: 'New York'
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb']);

      items = await db.findItems('People', {
        query: { country: 'USA' }, order: 'city', endBefore: 'New York'
      });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc']);
    });

    test('find a limited number of items', async function() {
      let items = await db.findItems('People', {
        query: { country: 'France' }, limit: 2
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd']);
    });

    test('find items using an index projection', async function() {
      let items = await db.findItems('People', {
        query: { country: 'France' }, properties: ['firstName', 'lastName']
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, { firstName: 'Manuel', lastName: 'Vila' });

      items = await db.findItems('People', { // will not use projection
        query: { country: 'France' }, properties: ['firstName', 'lastName', 'age']
      });
      keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
    });

    test('find items using a computed index', async function() {
      let items = await db.findItems('People', { order: 'fullNameSortKey' });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    test('count all items in a table', async function() {
      let count = await db.countItems('People');
      assert.strictEqual(count, 6);
    });

    test('count items with a query', async function() {
      let count = await db.countItems('People', {
        query: { age: 39 }
      });
      assert.strictEqual(count, 2);

      count = await db.countItems('People', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      count = await db.countItems('People', {
        query: { country: 'France', city: 'Paris' }
      });
      assert.strictEqual(count, 1);

      count = await db.countItems('People', {
        query: { country: 'Japan', city: 'Tokyo' }
      });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', async function() {
      let keys = [];
      await db.forEachItems('People', { batchSize: 2 }, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('iterate over items in a specific order', async function() {
      let keys = [];
      let options = { order: ['lastName', 'firstName'], batchSize: 2 };
      await db.forEachItems('People', options, async function(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    test('find and delete items', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = await db.findAndDeleteItems('People', options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = await db.findItems('People');
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);
      deletedItemsCount = await db.findAndDeleteItems('People', options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    test('change an item inside a transaction', async function() {
      assert.isFalse(db.isInsideTransaction);
      await db.transaction(async function(tr) {
        assert.isTrue(tr.isInsideTransaction);
        let innerItem = await tr.getItem('People', 'aaa');
        assert.strictEqual(innerItem.firstName, 'Manuel');
        innerItem.firstName = 'Manu';
        await tr.putItem('People', 'aaa', innerItem);
        innerItem = await tr.getItem('People', 'aaa');
        assert.strictEqual(innerItem.firstName, 'Manu');
      });
      let item = await db.getItem('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manu');
    });

    test('change an item inside an aborted transaction', async function() {
      try {
        assert.isFalse(db.isInsideTransaction);
        await db.transaction(async function(tr) {
          assert.isTrue(tr.isInsideTransaction);
          let innerItem = await tr.getItem('People', 'aaa');
          assert.strictEqual(innerItem.firstName, 'Manuel');
          innerItem.firstName = 'Manu';
          await tr.putItem('People', 'aaa', innerItem);
          innerItem = await tr.getItem('People', 'aaa');
          assert.strictEqual(innerItem.firstName, 'Manu');
          throw new Error('something wrong');
        });
      } catch (err) {
        // noop
      }
      let item = await db.getItem('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manuel');
    });
  }); // rich database suite
});
