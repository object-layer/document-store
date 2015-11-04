'use strict';

import fnName from 'fn-name';
import isPlainObject from 'lodash.isplainobject';

export class Index {
  constructor(options = {}) {
    if (!isPlainObject(options)) options = { properties: options };
    this.properties = this.normalizeProperties(options.properties);
    if (options.projection != null) this.projection = options.projection;
    if (options.version != null) this.version = options.version;
  }

  get keys() {
    return this.properties.map(property => property.key);
  }

  normalizeProperties(properties) {
    if (!Array.isArray(properties)) properties = [properties];
    properties = properties.map(property => {
      if (typeof property === 'string') { // simple index
        return { key: property, value: true };
      } else if (typeof property === 'function') { // computed index
        let key = fnName(property);
        if (!key) throw new Error('Invalid index definition: computed index function cannot be anonymous. Use a named function or set the displayName function property.');
        return { key, value: property };
      } else {
        throw new Error('Invalid index definition');
      }
    });
    return properties;
  }

  toJSON() {
    let json = { keys: this.keys };
    if (this.projection != null) json.projection = this.projection;
    if (this.version != null) json.version = this.version;
    return json;
  }
}

export default Index;
