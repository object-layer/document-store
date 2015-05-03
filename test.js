"use strict";

require('co-mocha');
var assert = require('chai').assert;
var _ = require('lodash');
var util = require('kinda-util').create();
var KindaDB = require('./');

suite('KindaDB', function() {
  var catchError = function *(fn) {
    var err;
    try {
      yield fn();
    } catch (e) {
      err = e
    }
    return err;
  };

  suite('migrations', function() {
    test('one empty table', function *() {
      var stats;
      try {
        var db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' }
        ]);

        stats = yield db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 0);

        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 1);
      } finally {
        yield db.destroyDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 0);
      }
    });

    test('one item in a table', function *() {
      var stats;
      try {
        var db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' }
        ]);
        yield db.putItem('Table1', 'aaa', { property1: 'value1' });
        stats = yield db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 2);
      } finally {
        yield db.destroyDatabase();
      }
    });

    test('one table added afterwards then removed', function *() {
      var db, stats;
      try {
        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' }
        ]);
        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);

        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' },
          { name: 'Table2' }
        ]);
        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.tablesCount, 2);

        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table2' }
        ]);
        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);
        assert.strictEqual(stats.removedTablesCount, 1);

        yield db.removeTablesMarkedAsRemoved();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.tablesCount, 1);
        assert.strictEqual(stats.removedTablesCount, 0);
      } finally {
        yield db.destroyDatabase();
      }
    });

    test('one index added afterwards then removed', function *() {
      var db, stats;
      try {
        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' }
        ]);
        yield db.putItem('Table1', 'aaa', { property1: 'value1' });
        stats = yield db.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 2);

        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1', indexes: ['property1'] }
        ]);
        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.indexesCount, 1);
        assert.strictEqual(stats.store.pairsCount, 3);

        yield db.putItem('Table1', 'bbb', { property1: 'value2' });
        stats = yield db.getStatistics();
        assert.strictEqual(stats.store.pairsCount, 5);

        db = KindaDB.create('Test', 'mysql://test@localhost/test', [
          { name: 'Table1' }
        ]);
        yield db.initializeDatabase();
        stats = yield db.getStatistics();
        assert.strictEqual(stats.indexesCount, 0);
        assert.strictEqual(stats.store.pairsCount, 3);
      } finally {
        yield db.destroyDatabase();
      }
    });
  }); // migrations suite

  suite('simple database', function() {
    var db;

    suiteSetup(function *() {
      db = KindaDB.create('Test', 'mysql://test@localhost/test', [
        { name: 'Users' }
      ]);
    });

    suiteTeardown(function *() {
      yield db.destroyDatabase();
    });

    test('tables definition', function *() {
      assert.strictEqual(db.tables.length, 1);

      var table = db.tables[0];
      assert.strictEqual(table.name, 'Users');
      assert.strictEqual(table.indexes.length, 0);
    });

    test('put, get and delete some items', function *() {
      yield db.putItem('Users', 'mvila', { firstName: 'Manu', age: 42 });
      var user = yield db.getItem('Users', 'mvila');
      assert.deepEqual(user, { firstName: 'Manu', age: 42 });
      yield db.deleteItem('Users', 'mvila');
      var user = yield db.getItem('Users', 'mvila', { errorIfMissing: false });
      assert.isUndefined(user);
    });
  }); // simple database suite

  suite('rich database', function() {
    var db;

    suiteSetup(function *() {
      db = KindaDB.create('Test', 'mysql://test@localhost/test', [
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
      ]);
    });

    suiteTeardown(function *() {
      yield db.destroyDatabase();
    });

    setup(function *() {
      yield db.putItem('People', 'aaa', {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
      yield db.putItem('People', 'bbb', {
        firstName: 'Jack', lastName: 'Daniel',
        age: 60, city: 'New York', country: 'USA'
      });
      yield db.putItem('People', 'ccc', {
        firstName: 'Bob', lastName: 'Cracker',
        age: 20, city: 'Los Angeles', country: 'USA'
      });
      yield db.putItem('People', 'ddd', {
        firstName: 'Vincent', lastName: 'Vila',
        age: 43, city: 'Céret', country: 'France'
      });
      yield db.putItem('People', 'eee', {
        firstName: 'Pierre', lastName: 'Dupont',
        age: 39, city: 'Lyon', country: 'France'
      });
      yield db.putItem('People', 'fff', {
        firstName: 'Jacques', lastName: 'Fleur',
        age: 39, city: 'San Francisco', country: 'USA'
      });
    });

    teardown(function *() {
      yield db.deleteItem('People', 'aaa', { errorIfMissing: false });
      yield db.deleteItem('People', 'bbb', { errorIfMissing: false });
      yield db.deleteItem('People', 'ccc', { errorIfMissing: false });
      yield db.deleteItem('People', 'ddd', { errorIfMissing: false });
      yield db.deleteItem('People', 'eee', { errorIfMissing: false });
      yield db.deleteItem('People', 'fff', { errorIfMissing: false });
    });

    test('tables definition', function *() {
      assert.strictEqual(db.tables.length, 1);
      var table = db.tables[0];
      assert.strictEqual(table.name, 'People');
      assert.strictEqual(table.indexes.length, 5);
      assert.strictEqual(table.indexes[0].properties.length, 2);
      assert.strictEqual(table.indexes[0].properties[0].key, 'lastName');
    });

    test('get many items', function *() {
      var items = yield db.getItems('People', ['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[1].key, 'ccc');
      assert.strictEqual(items[1].value.firstName, 'Bob');
    });

    test('find all items in a table', function *() {
      var items = yield db.findItems('People');
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.firstName, 'Manuel');
      assert.strictEqual(items[5].key, 'fff');
      assert.strictEqual(items[5].value.firstName, 'Jacques');
    });

    test('find and order items', function *() {
      var items = yield db.findItems('People', { order: 'age' });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'ccc');
      assert.strictEqual(items[0].value.age, 20);
      assert.strictEqual(items[5].key, 'bbb');
      assert.strictEqual(items[5].value.age, 60);

      var items = yield db.findItems('People', { order: 'age', reverse: true });
      assert.strictEqual(items.length, 6);
      assert.strictEqual(items[0].key, 'bbb');
      assert.strictEqual(items[0].value.age, 60);
      assert.strictEqual(items[5].key, 'ccc');
      assert.strictEqual(items[5].value.age, 20);

      var err = yield catchError(function *() {
        yield db.findItems('People', { order: 'missingProperty' });
      });
      assert.instanceOf(err, Error);
    });

    test('find items with a query', function *() {
      var items = yield db.findItems('People', { query: { country: 'France' } });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);

      var items = yield db.findItems('People', { query: { country: 'USA' } });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);

      var items = yield db.findItems('People', { query: { city: 'New York', country: 'USA' } });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb']);

      var items = yield db.findItems('People', { query: { country: 'Japan' } });
      assert.strictEqual(items.length, 0);
    });

    test('find items with a query and an order', function *() {
      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city'
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'fff']);

      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city', reverse: true
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['fff', 'bbb', 'ccc']);
    });

    test('find items after a specific item', function *() {
      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city', start: 'New York'
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'fff']);

      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city', startAfter: 'New York'
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['fff']);
    });

    test('find items before a specific item', function *() {
      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city', end: 'New York'
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb']);

      var items = yield db.findItems('People', {
        query: { country: 'USA' }, order: 'city', endBefore: 'New York'
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc']);
    });

    test('find a limited number of items', function *() {
      var items = yield db.findItems('People', {
        query: { country: 'France' }, limit: 2
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd']);
    });

    test('find items using an index projection', function *() {
      var items = yield db.findItems('People', {
        query: { country: 'France' }, properties: ['firstName', 'lastName']
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, { firstName: 'Manuel', lastName: 'Vila' });

      var items = yield db.findItems('People', { // will not use projection
        query: { country: 'France' }, properties: ['firstName', 'lastName', 'age']
      });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['aaa', 'ddd', 'eee']);
      assert.deepEqual(items[0].value, {
        firstName: 'Manuel', lastName: 'Vila',
        age: 42, city: 'Paris', country: 'France'
      });
    });

    test('find items using a computed index', function *() {
      var items = yield db.findItems('People', { order: 'fullNameSortKey' });
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    test('count all items in a table', function *() {
      var count = yield db.countItems('People');
      assert.strictEqual(count, 6);
    });

    test('count items with a query', function *() {
      var count = yield db.countItems('People', {
        query: { age: 39 }
      });
      assert.strictEqual(count, 2);

      var count = yield db.countItems('People', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      var count = yield db.countItems('People', {
        query: { country: 'France', city: 'Paris' }
      });
      assert.strictEqual(count, 1);

      var count = yield db.countItems('People', {
        query: { country: 'Japan', city: 'Tokyo' }
      });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', function *() {
      var keys = [];
      yield db.forEachItems('People', { batchSize: 2 }, function *(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('iterate over items in a specific order', function *() {
      var keys = [];
      var options = { order: ['lastName', 'firstName'], batchSize: 2 };
      yield db.forEachItems('People', options, function *(value, key) {
        keys.push(key);
      });
      assert.deepEqual(keys, ['ccc', 'bbb', 'eee', 'fff', 'aaa', 'ddd']);
    });

    test('find and delete items', function *() {
      var options = { query: { country: 'France' }, batchSize: 2 };
      yield db.findAndDeleteItems('People', options);
      var items = yield db.findItems('People');
      var keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'fff']);
    });

    test('change an item inside a transaction', function *() {
      assert.isFalse(db.isInsideTransaction());
      yield db.transaction(function *(tr) {
        assert.isTrue(tr.isInsideTransaction());
        var item = yield tr.getItem('People', 'aaa');
        assert.strictEqual(item.firstName, 'Manuel');
        item.firstName = 'Manu';
        yield tr.putItem('People', 'aaa', item);
        var item = yield tr.getItem('People', 'aaa');
        assert.strictEqual(item.firstName, 'Manu');
      });
      var item = yield db.getItem('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manu');
    });

    test('change an item inside an aborted transaction', function *() {
      try {
        assert.isFalse(db.isInsideTransaction());
        yield db.transaction(function *(tr) {
          assert.isTrue(tr.isInsideTransaction());
          var item = yield tr.getItem('People', 'aaa');
          assert.strictEqual(item.firstName, 'Manuel');
          item.firstName = 'Manu';
          yield tr.putItem('People', 'aaa', item);
          var item = yield tr.getItem('People', 'aaa');
          assert.strictEqual(item.firstName, 'Manu');
          throw new Error('something wrong');
        });
      } catch (err) {
      }
      var item = yield db.getItem('People', 'aaa');
      assert.strictEqual(item.firstName, 'Manuel');
    });
  }); // rich database suite
});
