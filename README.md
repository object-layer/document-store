# DocumentStore [![Build Status](https://travis-ci.org/object-layer/document-store.svg?branch=master)](https://travis-ci.org/object-layer/document-store)

Document store on top of any database.

### Why this module?

Document stores offer a very good developer experience. Take MongoDB for example, the API is straightforward, the data structure is quite flexible and the amount of storage can scale horizontally in a rather good way.

So, what's wrong?

First, I don't know how you guys are doing but the lack of transaction is a big concern for me. There are many cases where we have objects with their own identity (therefore not aggregatable) and strong connections between them. To sleep well at night I have to be sure about the integrity of my data.

Second, I have a little problem of commitment. Choosing a database is not a small matter. If I go on MongoDB today and I want to switch to something else in the future, the transition could be painful. The smartest choices are those that engage us as less as possible. When I select something as important as a database, I want to choose a set of features and an API but not a particular implementation.

That's why I created this module which is nothing but a layer on top of [KeyValueStore](https://www.npmjs.com/package/key-value-store), a simple module abstracting any kind of transactional key-value store.

### Features

- Simple and beautiful API.
- Secondary indexes (simple, compound and computed).
- Projections for blazing fast queries.
- Automatic migrations.
- Easy ACID transactions with implicit begin/commit/rollback.
- ES7 `async`/`await` ready.
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

A document is nothing more than a JavaScript object serializable by `JSON.stringify`. To customize the serialization, you may want to implement the `toJSON()` method on your documents.

### Promise based API

Every asynchronous operation returns a promise. You can handle them as is but I think it is a lot better to consume them with the fantastic ES7 `async`/`await` feature. Since ES7 is not really there yet, you should compile your code with something like [Babel](https://babeljs.io/).

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
- `url`: the URL where your data is stored. Internally, a [KeyValueStore](https://www.npmjs.com/package/key-value-store) is created with that same URL targeting the actual data storage backend.
- `collections`: an array of collection definitions. A collection definition can be either a string or an object. In case of a string, it is simply the name of the collection. In case of an object, the properties are:
  - `name`: the name of the collection.
  - `indexes` _(optional)_: an array of index definitions. An index definition is an object with the following attributes:
      - `properties`: an array of properties from which the index is created. A property can be either a string or a function. In case of a string, it is a _path_ to a property in the indexed documents. A path can be a simple key (e.g. `'country'`) or a nesting of keys (e.g. `'postalAddress.country'`). Finally, the indexed data can be computed from a function (see examples bellow).
      - `projection` _(optional)_: an array of document properties to project into the index. This option, in exchange for an increase of size of the indexes, significantly speeds up queries when the `find()` method is used with the `properties` option.
      - `version` _(optional)_: this option is useful in conjunction with computed properties. Since the migration engine cannot detect changes made inside functions, it is unable to automatically rebuild indexes when necessary. So, when you change the logic of a computed property, you can increment the `version` option to force the reindexing.
- `log` _(optional)_: an instance of [UniversalLog](https://www.npmjs.com/package/universal-log) used by the document store when important events occur.

#### Example of index definitions

```javascript
let store = new DocumentStore({
  name: 'MyCoolProject',
  url: 'mysql://test@localhost/test',
  collections: [
    'Countries', // no indexes
    {
      name: 'People',
      indexes: [
        'age', // simple index
        ['lastName', 'firstName'], // compound index
        {
          properties: [
            function sortKey(doc) { // computed index
              return doc.lastName && doc.lastName.toLowerCase();
            }
          ],
          version: 1 // to increment if the function changes
        },
        {
          properties: ['createdOn'],
          projection: ['firstName', 'lastName', 'age'] // projection for fast queries
        }
      ]
    }
  ]
});
```

### `store.get(collection, key, [options])`

Get a document from the store.

```javascript
let person = await store.get('People', 'abc123');
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown when the specified `key` is missing from the store. If `false`, the method returns `undefined` when the `key` is missing.

### `store.put(collection, key, doc, [options])`

Put a document in the store.

```javascript
await store.put('People', 'abc123', { name: 'John', age: 42 });
```

#### `options`

- `createIfMissing` _(default: `true`)_: if `false`, an error is thrown when the specified `key` is missing from the store ("update" semantic).
- `errorIfExists` _(default: `false`)_: if `true`, an error is thrown when the specified `key` is already present in the store ("create" semantic).

### `store.delete(collection, key, [options])`

Delete a document from the store.

```javascript
let hasBeenDeleted = await store.delete('People', 'abc123');
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown when the specified `key` is missing from the store. If `false`, the method returns `false` in case the `key` is missing.

### `store.getMany(collection, keys, [options])`

Get several document from the store. Return an array of objects with two properties: `key` and `document`. The order of the specified `keys` is preserved in the result.

```javascript
let people = await store.getMany('People', ['abc123', 'def789', /* ... */]);
```

#### `options`

- `errorIfMissing` _(default: `true`)_: if `true`, an error is thrown if one of the specified `keys` is missing from the store.

### `store.find(collection, [options])`

Find documents matching the specified criteria. Return an array of objects with two properties: `key` and `document`.

```javascript
// Find everyone
let people = await store.find('People');

// Find people living in Tokyo
let people = await store.find('People', { query: { city: 'Tokyo' } });

// Find all single females between 30 and 40
let people = await store.find('People', {
  query: { gender: 'female', status: 'single' },
  order: ['age'],
  start: 30,
  end: 40
});
```

#### `options`

- `query`: an object of key-value pairs corresponding to the search criteria.
- `order`: an array of property names specifying the sort order. When no `order` is specified, the returned items are sorted by key.
- `start`, `startAfter`: when you specify the `order` option, you can restrict the returned items to those greater (or equal) the specified values. When no `order` is specified, you can use the `start` and `startAfter` options to fetch only the items starting with a certain `key`. Finally, since the items are always sorted by `order` and then by `key`, you can specify both at the same time (e.g. `['Tokyo', 'abc123']`).
- `end`, `endBefore`: similar to `start`, `startAfter` but for the less than (or equal) condition.
- `reverse` _(default: `false`)_: if `true`, reverse the order of returned items.
- `limit` _(default: `50000`)_: limit the number of returned items to the specified value.
- `properties` _(default: `'*'`)_: an array of property names or the `'*'` string. If `'*'` is specified (the default), all document properties are fetched. Otherwise, only the specified properties are fetched. Used in conjunction with a `projection`, you can significantly speed up queries.

Note: the property names specified in the `query` and `order` options should match an existing index, otherwise the method will throw an error. For example, if you have `{ gender: 'female', status: 'single' }` as `query` and `['age']` as `order`, you should have a compound index with `['female', 'status', 'age']` properties in your collection.

### `store.count(collection, [options])`

Count the number of documents matching the specified criteria.

```javascript
let peopleCount = await store.find('People', {
  query: { city: 'Tokyo', country: 'Japan' }
});
```

#### `options`

Same options as the `find()` method (excepted `reverse` and `properties` which are useless in the context of a count).

### `store.findAndDelete(collection, [options])`

Delete documents matching the specified criteria. Return the number of deleted documents.

```javascript
let deletedDocsCount = await store.findAndDelete('People', {
  query: { country: 'France' }
});
```

#### `options`

Same options as the `find()` method (excepted the `properties` option which is useless in the context of a deletion).

### `store.forEach(collection, options, fn, [thisArg])`

Run a function for each document matching the specified criteria. The function is called with `thisArg` as `this` context and receives two parameters: the document and the key.

```javascript
await store.forEach(
  'People',
  { query: { country: 'Japan' } },
  function(person, key) {
    console.log(person.name);
  }
);
```

#### `options`

Same options as the `find()` method with the addition of:

- `batchSize` _(default: `250`)_: maximum number of documents to fetch at the same time. Internally, the `find()` method is used to fetch the documents and the `batchSize` option is used to limit the number of documents fetched by each `find()` call.

### `store.transaction(fn)`

Run the specified function inside a transaction. The function receives a transaction handler as first argument. This handler should be used as a replacement of the document store for every operation made during the execution of the transaction. If any error occurs, the transaction is aborted and the document store is automatically rolled back.

```javascript
// Increment a counter
await store.transaction(async function(transaction) {
  let counter = await transaction.get('Counters', 'abc123');
  counter.value++;
  await transaction.put('Counters', 'abc123', counter);
});
```

### `store.close()`

Close all connections to the document store.

```javascript
await store.close();
```

## To do

- Collection renaming.
- More tests and better documentation (help wanted!).

## License

MIT
