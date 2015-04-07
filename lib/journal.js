'use strict'

let path = require('path')
let co = require('co')
let costream = require('co-stream')
let debug = require('debug')('prevalence')
let fs = require('mz/fs')
let JsonStream = require('json-stream')
let thenify = require('thenify')
let mkdirp = thenify(require('mkdirp'))

module.exports = function (journalPath) { return new Journal(journalPath) }

function Journal (journalPath) {
  this.journalPath = journalPath
}

let proto = Journal.prototype

proto.append = function Journal$append (line) {
  let journalPath = this.journalPath
  return co(function * () {
    try {
      return yield appendLine(this.journalPath, line)
    } catch (e) {
      if (e && e.code === 'ENOENT') {
        // the journal file doesnt exists nor can it be created
        // assume its cuz a missing folder
        debug('creating journalling folder')
        yield mkdirp(path.dirname(journalPath))
        return yield appendLine(this.journalPath, line)
      }
      throw e
    }
  }.bind(this))
}

proto.replay = function Journal$replay (handler) {
  debug('replaying journal')

  let journalPath = this.journalPath

  return new Promise(function (resolve, reject) {
    function onReadStreamError (err) {
      if (err && err.code === 'ENOENT') {
        debug('no journal to replay from. skipping.')
        return resolve(0)
      }
      reject(err)
    }

    let replayCount = 0
    fs.createReadStream(journalPath)
      .on('error', onReadStreamError)
      .pipe(new JsonStream())
      .on('error', reject)
      .pipe(costream.each(function * (record) {
        yield handler(record)
        ++replayCount
      }, {
        objectMode: true,
        parallell: 1
      }))
      .on('error', reject)
      .on('finish', function () {
        resolve(replayCount)
      })
  })
  .then(function (replayCount) {
    debug('replay done. replayed commands: %s', replayCount)
    return true
  })
  .catch(function (err) {
    debug('replay failed: %s', err)
    throw err
  })
}

function appendLine (journalPath, line) {
  return fs.appendFile(journalPath, line + '\n', {
    encoding: 'utf8'
  })
}
