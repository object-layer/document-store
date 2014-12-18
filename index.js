'use strict';

var config = require('kinda-config').create();

var Factory = {
  create: function(name, url, options) {
    if (!name) name = config['kinda-db'] && config['kinda-db'].name;
    if (!name) throw new Error('name is missing');
    if (!url) url = config['kinda-db'] && config['kinda-db'].url;
    if (!url) throw new Error('url is missing');
    if (!options) options = config['kinda-db'] && config['kinda-db'].options || {};
    var pos = url.indexOf(':');
    if (pos === -1) throw new Error('invalid url');
    var protocol = url.substr(0, pos);
    switch (protocol) {
    case 'mysql':
    case 'websql':
      return require('kinda-db-store').create(name, url, options);
    case 'http':
    case 'https':
      return require('kinda-db-rest').create(name, url, options);
    default:
      throw new Error('unknown protocol');
    }
  }
};

module.exports = Factory;
