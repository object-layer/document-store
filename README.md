# DocumentStore [![Build Status](https://travis-ci.org/object-layer/document-store.svg?branch=master)](https://travis-ci.org/object-layer/document-store)

Document store on top of any database.

### Why this module?

Document stores offer a very good developer experience. Take MongoDB for example, the API is straightforward, the data structure is quite flexible and the amount of storage can scale horizontally in a rather good way.

So, what's wrong?

First, I don't know how you guys are doing but the lack of transaction is a big concern for me. There are many cases where we have objects with their own identity (therefore not aggregatable) and strong connections between them. To sleep well at night I have to be sure about the integrity of my data.

Second, I have a little problem of commitment. Choosing a database is not a small matter. If I go on MongoDB today and I want to switch to something else in the future, the transition could be quite painful. The smartest choices are those that engage us as less as possible. When I select something as important as a database, I would like to choose a set of features and an API but not a particular implementation.

That's why I created this module which is nothing but a layer on top of [KeyValueStore](https://www.npmjs.com/package/key-value-store), a simple module abstracting any kind of transactional key-value store.

### Features

- Simple and beautiful API.
- Secondary indexes (simple, compound and computed).
- Projections for really fast queries.
- Automatic migrations.
- Easy ACID transactions with implicit begin/commit/rollback.
- ES7 async/await ready.
- Works in Node and browser.

## Installation

```
npm install --save document-store
```

## Usage

### Simple operations

```javascript
import DocumentStore from 'document-store';

let store = new DocumentStore({
  name: 'MyCoolProject',
  url: 'mysql://test@localhost/test',
  collections: ['People']
});

async function simple() {
  // Create
  await store.put('People', 'abc123', { name: 'John', age: 42 });

  // Read
  let person = await store.get('People', 'abc123');

  // Update
  person.age++;
  await store.put('People', 'abc123', person);

  // Delete
  await store.delete('People', 'abc123');
}
```

### Indexes and queries

```javascript
import DocumentStore from 'document-store';

let store = new DocumentStore({
  name: 'MyCoolProject',
  url: 'mysql://test@localhost/test',
  collections: [{
    name: 'People',
    indexes: ['name', 'age']
  }]
});

async function query() {
  // Find all John older than 40
  let people = await store.find('People', {
    query: { name: 'John' },
    order: ['age'],
    startAfter: 40
  });
}
```

### Transactions

```javascript
import DocumentStore from 'document-store';

let store = new DocumentStore({
  name: 'MyCoolProject',
  url: 'mysql://test@localhost/test',
  collections: ['People']
});

async function criticalOperation() {
  await store.transaction(async function(transaction) {
    let person = await transaction.get('People', 'abc123');
    person.age++;
    await transaction.put('People', 'abc123', person);
    // ...
    // if no error has been thrown, the transaction is automatically committed
  });
}
```

## Basic concepts

### Collections, documents and keys

Collections are useful to group documents of the same kind but there is no predefined schema.

Every document has a unique key which can be either a string or a number.

A document is nothing more than a JavaScript object serializable by `JSON.stringify`. To customize the serialization, you may want to implement a `toJSON()` method on your documents.

### Promise based API

Every asynchronous operation returns a promise. You can handle them as is but I think it is a lot better to consume them with the fantastic ES7 async/await feature. Since ES7 is not really there yet, you should compile your code with something like [Babel](https://babeljs.io/).

## API

### `new DocumentStore(options)`

Create a document store.

```javascript
import DocumentStore from 'document-store';

let store = new DocumentStore(
  name: 'MyCoolProject',
  url: 'mysql://test@localhost/test',
  collections: ['People']
);
```

#### `options`

- `name`: the name of the document store to create.
- `url`: the URL where you want to store the data. Internally, a [KeyValueStore](https://www.npmjs.com/package/key-value-store) is created with that same URL targeting the actual data storage backend.
- `collections`: an array of collection definitions. A collection definition can be either a string or an object. In case of a string, it is simply the name of the collection. In case of an object, the following properties are available:
  - `name`: the name of the collection.
  - `indexes` _(optional)_: an array of index definitions. An index definition is an object with the following attributes:
      - `properties`: an array of properties from which the index is created. A property can be either a string or a function. In case of a string, it is a _path_ to a property in the indexed documents. A path can be a simple key (e.g. `'country'`) or a nesting of keys (e.g. `'postalAddress.country'`). Finally, the indexed data can be computed from a function (see examples bellow).
      - `projection` _(optional)_: an array of document properties to project in the index. This option, in exchange for an increase of size of the indexes, significantly speeds up queries when the `find()` method is used with the `properties` option.
      - `version` _(optional)_: this option is useful in conjunction with computed properties. Since the migration engine cannot detect changes made inside functions, it is unable to automatically rebuild indexes when necessary. So, when you change the logic of a computed property, you can increment the `version` option to force the reindexing.
- `log` _(optional)_: an instance of [UniversalLog](https://www.npmjs.com/package/universal-log) used by the document store when important events occur.

#### `options`

#### `Collection definition`

A collection definition can be either a string or an object. In case of a string, it represents the name of the collection. In case of an object, the following properties are available:

- `name`: the name of the document store to create.

### `store.get(key, [options])`

Get an item from the store.

```javascript
let user = await store.get(['users', 'abc123']);
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown if the specified `key` is missing from the store. If `false`, the method returns `undefined` when the `key` is missing.

### `store.put(key, value, [options])`

Put an item in the store.

```javascript
await store.put(['users', 'abc123'], { firstName: 'Manu', age: 42 });
```

#### `options`

- `createIfMissing` _(default: `true`)_: if `false`, an error is thrown if the specified `key` is missing from the store. This way you can ensure an "update" semantic.
- `errorIfExists` _(default: `false`)_: if `true`, an error is thrown if the specified `key` is already present in the store. This way you can ensure a "create" semantic.

### `store.delete(key, [options])`

Delete an item from the store.

```javascript
await store.delete(['users', 'abc123']);
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown if the specified `key` is missing from the store. If `false`, the method returns `false` when the `key` is missing.

### `store.getMany(keys, [options])`

Get several items from the store. Return an array of objects composed of two properties: `key` and `value`. The order of the specified `keys` is preserved in the result.

```javascript
let users = await store.getMany([
  ['users', 'abc123'],
  ['users', 'abcde67890'],
  // ...
]);
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown if one of the specified `keys` is missing from the store.
- `returnValues` _(default: `true`)_: if `false`, only keys found in the store are returned (no `value` property).

### `store.putMany(items, [options])`

Not implemented yet.

### `store.deleteMany(keys, [options])`

Not implemented yet.

### `store.find([options])`

Fetch items matching the specified criteria. Return an array of objects composed of two properties: `key` and `value`. The returned items are ordered by key.

```javascript
// Fetch all users
let users = await store.find({ prefix: 'users' });

// Fetch 30 users after the 'abc123' key
let users = await store.find({
  prefix: 'users',
  startAfter: 'abc123',
  limit: 30
});
```

#### `options`

- `prefix`: fetch items with keys starting with the specified value.
- `start`, `startAfter`: fetch items with keys greater than (or equal to if you use the `start` option) the specified value.
- `end`, `endBefore`: fetch items with keys less than (or equal to if you use the `end` option) the specified value.
- `reverse` _(default: `false`)_: if `true`, reverse the order of returned items.
- `limit` _(default: `50000`)_: limit the number of fetched items to the specified value.
- `returnValues` _(default: `true`)_: if `false`, only keys found in the store are returned (no `value` property).

### `store.count([options])`

Count items matching the specified criteria.

```javascript
let users = await store.count({
  prefix: 'users',
  startAfter: 'abc123'
});
```

#### `options`

- `prefix`: count items with keys starting with the specified value.
- `start`, `startAfter`: count items with keys greater than (or equal to if you use the `start` option) the specified value.
- `end`, `endBefore`: count items with keys less than (or equal to if you use the `end` option) the specified value.

### `store.findAndDelete([options])`

Delete items matching the specified criteria. Return the number of deleted items.

```javascript
let users = await store.findAndDelete({
  prefix: 'users',
  startAfter: 'abc123'
});
```

#### `options`

- `prefix`: delete items with keys starting with the specified value.
- `start`, `startAfter`: delete items with keys greater than (or equal to if you use the `start` option) the specified value.
- `end`, `endBefore`: delete items with keys less than (or equal to if you use the `end` option) the specified value.

### `store.transaction(fun)`

Run the specified function inside a transaction. The function receives a transaction handler as first argument. This handler should be used as a replacement of the store for every operation made during the execution of the transaction. If any error occurs, the transaction is aborted and the store is automatically rolled back.

```javascript
// Increment a counter
await store.transaction(async function(transaction) {
  let value = await transaction.get('counter');
  value++;
  await transaction.put('counter', value);
});
```

### `store.close()`

Close all connections to the store.

## License

MIT
