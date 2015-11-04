# DocumentStore [![Build Status](https://travis-ci.org/object-layer/document-store.svg?branch=master)](https://travis-ci.org/object-layer/document-store)

Document store on top of any database.

### Why this module?

Document stores offer a very good developer experience. Take MongoDB for example, the API is straightforward, the data structure is quite flexible and the amount of storage can scale horizontally in a rather good way.

So, what's wrong?

First, I don't know how you guys are doing but for me the lack of transaction is a big concern. There are many cases where we have objects with their own identity (therefore not aggregatable) and deep connections between them. If I want to sleep well at night I have to be sure about the integrity of my data, ACID transactions are here for that.

Second, I have a little problem of commitment. Choosing a database is not a small matter. If I go on MongoDB today and I want to switch to something else in the future, the transition will be very painful. The best technological choices are those that engage us as less as possible. When I choose something as important as a database, I would like to choose a set of features and an API but not a particular implementation.

That's why I created this module which is nothing but a layer on top of [KeyValueStore](https://www.npmjs.com/package/key-value-store), a simple module abstracting any kind of transactional key-value store.

### Features

- Simple and beautiful API.
- Secondary indexes (simple, compound and computed).
- Automatic migrations.
- Easy transactions with automatic begin/commit/rollback.
- ES7 async/await ready.
- Works in Node and browser.

## Documentation

```javascript
// TODO
```

## License

MIT
