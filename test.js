"use strict";

require('co-mocha');
var assert = require('chai').assert;
var KindaDB = require('./');

suite('KindaDB', function() {
  var db;

  suiteSetup(function *() {
    db = KindaDB.create('Test', 'mysql://test@localhost/test');

    db.registerMigration(1, function *() {
      yield this.addTable('Users');
    });

    yield db.initializeDatabase();
  });

  suiteTeardown(function *() {
    yield db.destroyDatabase();
  });

  test('getTables()', function *() {
    var tables = db.getTables();
    assert.strictEqual(tables.length, 1);
    assert.strictEqual(tables[0].name, 'Users');
  });

  test('simple put, get and del', function *() {
    yield db.put('Users', 'mvila', { firstName: 'Manu', age: 42 });
    var user = yield db.get('Users', 'mvila');
    assert.deepEqual(user, { firstName: 'Manu', age: 42 });
    yield db.del('Users', 'mvila');
    var user = yield db.get('Users', 'mvila', { errorIfMissing: false });
    assert.isUndefined(user);
  });
});
