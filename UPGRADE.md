# Upgrade to v0.2

## Renamed methods

* delIndex => removeIndex
* get => getItem
* put => putItem
* del => deleteItem
* getMany => getItems
* getRange => findItems
* getCount => countItems
* forRange => forEachItems
* delRange => findAndDeleteItems

## Parameter changes

* findItems, countItems, forEachItems and findAndDeleteItems: 'by' and 'prefix' options are replaced with 'query' and 'order'
