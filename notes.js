var db = KindaDB.create('Test', 'mysql://...', [
  {
    name: 'People',
    indexes: [
      'age',
      ['country', 'city'],
      {
        properties: ['lastName', 'firstName'],
        projection: ['firstName', 'lastName', 'age']
      },
      function(item) {
          return ...;
      }
    ]
  }
]);

// ---------------------

var KindaDB = require('kinda-db');

var db = KindaDB.create('Durable', storeURL);

db.registerMigration(1, function *() {
  yield this.addTable('Vaults');
  yield this.addIndex('Vaults', 'number');
  yield this.addIndex('Vaults', 'balance');
});

db.registerMigration(2, function *() {
  yield vaults.delIndex('Vaults', 'balance');
});

module.exports = db;

// ---------------------

var vault = yield db.get('Vaults', 'due7k9da');

yield db.put('Vaults', 'due7k9da', vault);

yield db.del('Vaults', 'due7k9da');

var items = yield db.getRange('Vaults', { startAfter: 'due7k9da' });

var items = yield db.getRange('Vaults', { index: 'number', startAfter: 230 });

// ---------------------

// Stockage dans le store :

['Durable'] = {
  version: 1,
  isLocked: false,
  lastMigrationNumber: 2,
  tables: [
    {
      name: 'Vaults',
      indexes: [
        { name: 'number', keys: ['number'] }
        { name: 'type+createdOn', keys: ['type', 'createdOn'] }
      ]
    }
  ]
}

['Durable', 'Vaults', 'due7k9da'] = { ... }

['Durable', 'Vaults:number', 120, 'due7k9da'] = null;

['Durable', 'Vaults:type+createdOn', 'premium', '1/1/14', 'due7k9da'] = null;

////////////////////////////////////////////

var StoreDatabase = require('kinda-db/store');

var db = StoreDatabase.create('Durable', 'mysql://');

// ---

var RESTDatabase = require('kinda-db/rest');

var db = RESTDatabase.create('Durable', 'http://...');

// ---------------------

yield db.get('vaults', 'xjd6djd');
// => GET /vaults/xjd6djd

yield db.put('vaults', undefined, { ... }, { errorIfExists: true });
// => POST /vaults

yield db.put('vaults', 'xjd6djd', { ... }, { createIfMissing: false });
// => PUT /vaults/xjd6djd

yield db.del('vaults', 'xjd6djd', { errorIfMissing: true });
// => DELETE /vaults/xjd6djd

yield db.getRange('vaults');
// => GET /vaults

yield db.getRange('vaults', { by: 'number' });
// => GET /vaults?by=number

yield db.getRange('vaults', { by: 'number', equal: 3 });
// => GET /vaults?by=number&equal=num!3

yield db.getRange('files', { by: 'vaultId', equal: 'xjd6djd' });
// => GET /files?by=vaultId&equal=xjd6djd

////////////////////////////////////////////

// ---------------------

items = yield db.find('Vaults', { number: 120 });

items = yield db.find('Vaults', { $key: { '>=': 'due7k9da' } });

items = yield db.find('Vaults',
  { type: 'premium', createdOn: { '>=': '1/1/14' } },
  { reverse: true, limit: 30 }
);

////////////////////////////////////////////////////

// backend/collections/vaults.js

var db = require('../database');

var Vaults = Collection.extend('Vaults', function() {
  this.include(require('kinda-collection/kinda-db'));
  this.setTable(db.getTable('Vaults'));
});

// backend example

var context = Context.create();
var vaults = context.create(Vaults);
yield vaults.transaction(function *() {
  var vault = yield vaults.getItem('123');
  var file = vault.files.getItem('abc');
  file.status = 'available';
  yield file.save();
}, this);

// frontend example

var context = Context.create();
context.accessToken = 'skjdgh';
var vaults = context.create(Vaults);
var vault = yield vaults.getItem('123');

////////////////////////////////////////////////////

var store = FDBStore.create();
var vaults = Vaults.create(store);
yield store.try(function *() {
  var vault = yield vaults.getItem('123');
  var file = vault.files.getItem('abc');
  file.status = 'available';
  yield file.save();
}, this);

////////////////////////////////////////////////////

var store = RESTStore.create();
store.setAccessToken('...');
var vaults = Vaults.create(store);
var vault = yield vaults.getItem('123');

var vaults = yield db.defineCollection('vaults');

////////////////////////////////////////////////////

var vaults = store.createTable({
  name: 'Durable.Vaults.v1',
  indexes: ['name']
});

var vault = yield vaults.createItem('123', { ... });

////////////////////////////////////////////////////

yield vaults.defineIndex('name');

var vault = yield vaults.getItem('123');

yield vaults.createItem('123', { ... });

yield vaults.updateItem('123', { ... }, { upsert: false });

yield vaults.deleteItem('123');
