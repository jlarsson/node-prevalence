'use strict'

let co = require('co')
let Promise = require('any-promise')
let RwLock = require('rwlock')

module.exports = function () { return new Lock() }

function Lock () {
  this.rwlock = new RwLock()
}

let proto = Lock.prototype

proto.readLock = function (fn) {
  return lock(this.rwlock, 'readLock', fn)
}

proto.writeLock = function (fn) {
  return lock(this.rwlock, 'writeLock', fn)
}

function lock (rwlock, type, fn) {
  return new Promise(function (resolve, reject) {
    rwlock[type](function (release) {
      co(fn)
        .then(function (res) {
          resolve(res)
          release()
        })
        .catch(function (err) {
          reject(err)
          release()
        })
    })
  })
}
