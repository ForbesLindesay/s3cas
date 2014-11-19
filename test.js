'use strict';

var assert = require('assert');
var connect = require('./');

if (!process.env.CONNECTION) {
  console.error('ERROR: You must set the CONNECTION environment variable in order to run tests.');
  console.error();
  process.exit(1);
}

var client = connect(process.env.CONNECTION);

client.put(new Buffer('Hello World')).then(function (id) {
  assert(typeof id === 'string');
  return client.get(id);
}).then(function (buffer) {
  assert(Buffer.isBuffer(buffer));
  assert(buffer.toString('utf8') === 'Hello World');
}).done(function () {
  console.log('tests passed')
});
