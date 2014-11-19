'use strict';

var crypto = require('crypto');
var http = require('http');
var knox = require('knox');
var Promise = require('promise');
var concat = require('concat-stream');

function getId(buffer, algorithm) {
  return crypto.createHash(algorithm).update(buffer).digest('hex');
}

module.exports = function connect(connection, options) {
  var c = new Client(connection, options);
  return {
    put: c.put.bind(c),
    get: c.get.bind(c),
    verify: c.verify.bind(c)
  };
}

function Client(connection, options) {
  if (typeof connection === 'string') {
    connection = {
      key: connection.split('@')[0],
      secret: connection.split('@')[1],
      bucket: connection.split('@')[2]
    };
  }
  if (!connection) {
    throw new Error('You must provide s3cas with a connection');
  }
  if (options) {
    for (var key in options) {
      if (key !== 'key' && key !== 'secret' && key !== 'bucket') {
        connection[key] = options[key];
      }
    }
  }
  this._knox = knox.createClient(connection);
  this._algorithm = connection.algorithm || 'sha512';
}
Client.prototype.verify = function (id) {
  return (new Promise(function (resolve, reject) {
    var hash = crypto.createHash(this._algorithm);
    this._knox.getFile(id, function (err, res) {
      if (err) {
        reject(err);
      } else {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          res.on('error', reject);
          res.on('data', function (data) {
            hash.update(data);
          });
          res.on('end', function () {
            if (id === hash.digest('hex')) {
              resolve(id);
            } else {
              reject(new Error('Hash verification failed'));
            }
          });
        } else {
          statusError(res, reject);
        }
      }
    });
  }.bind(this)));
};
Client.prototype.put = function (buffer) {
  return (new Promise(function (resolve, reject) {
    if (!Buffer.isBuffer(buffer)) {
      throw new Error('You can only put buffers');
    }
    var id = getId(buffer, this._algorithm);
    this._knox.putBuffer(buffer, id, function (err, res) {
      if (err) {
        reject(err);
      } else {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(id);
        } else {
          statusError(res, reject);
        }
      }
    });
  }.bind(this))).then(function (id) {
    return this.verify(id);
  }.bind(this));
};
Client.prototype.get = function (id) {
  return (new Promise(function (resolve, reject) {
    this._knox.getFile(id, function (err, res) {
      if (err) {
        reject(err);
      } else {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          res.on('error', reject);
          res.pipe(concat(resolve));
        } else {
          statusError(res, reject);
        }
      }
    });
  }.bind(this))).then(function (buffer) {
    if (id !== getId(buffer, this._algorithm)) {
      throw new Error('Id does not match hash for "' + id + '"');
    }
    return buffer;
  }.bind(this));
};

function statusError(res, reject) {
  var err = new Error('');
  var body = '';
  res.on('data', function (data) {
    body += data;
  });
  res.on('end', end);
  res.on('error', end);
  function end() {
    var code = /\<code\>([^\>]+)\<\/code\>/i.exec(body);
    var message = /\<message\>([^\>]+)\<\/message\>/i.exec(body);
    err.status = res.statusCode;
    if (code) {
      err.code = code[1];
    }
    if (message) {
      err.message = message[1];
    } else {
      err.message = 'AWS ' + http.STATUS_CODES[res.statusCode];
    }
    if (code) {
      err.message += ' (status: ' + res.statusCode +
        ', code: "' + code[1] + '")';
    } else {
      err.message += ' (status: ' + res.statusCode + ')';
    }
    console.dir(err);
    reject(err);
  }
}
