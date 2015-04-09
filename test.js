"use strict";

require('co-mocha');
var assert = require('chai').assert;
var _ = require('lodash');
var KindaDB = require('./');

suite('KindaDB', function() {
  var db;

  var catchError = function *(fn) {
    var err;
    try {
      yield fn();
    } catch (e) {
      err = e
    }
    return err;
  };

  suiteSetup(function *() {
    db = KindaDB.create('Test', 'mysql://test@localhost/test');

    db.registerMigration(1, function *() {
      yield this.addTable('Users');
    });

    db.registerMigration(2, function *() {
      yield this.addTable('People');
      yield this.addIndex('People', ['lastName', 'firstName']);
      yield this.addIndex('People', ['age']);
      yield this.addIndex(
        'People', ['country'], { projection: ['firstName', 'lastName'] }
      );
      yield this.addIndex('People', ['country', 'city']);
    });

    yield db.initializeDatabase();
  });

  suiteTeardown(function *() {
    yield db.destroyDatabase();
  });

  test('tables definition', function *() {
    var tables = db.getTables();
    assert.strictEqual(tables.length, 2);
    assert.strictEqual(tables[0].name, 'Users');
    assert.strictEqual(tables[1].name, 'People');
  });

  test('put, get and delete some items', function *() {
    yield db.putItem('Users', 'mvila', { firstName: 'Manu', age: 42 });
    var user = yield db.getItem('Users', 'mvila');
    assert.deepEqual(user, { firstName: 'Manu', age: 42 });
    yield db.deleteItem('Users', 'mvila');
    var user = yield db.getItem('Users', 'mvila', { errorIfMissing: false });
    assert.isUndefined(user);
  });

  suite('with many items', function() {
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
  });
});
