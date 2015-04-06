'use strict'

let Promise = require('native-or-bluebird')
let co = require('co')
let thenify = require('thenify')
let costream = require('co-stream')
let debug = require('debug')('prevalence')
let RwLock = require('rwlock')
let defaults = require('defaults')
let JsonStream = require('json-stream')
let fs = require('mz/fs')
let path = require('path')
let mkdirp = thenify(require('mkdirp'))
let util = require('util')
let makeError = require('make-error')

module.exports = function (options) {
  return new Repository(options)
}

let ConfigurationError = module.exports.ConfigurationError = makeError('ConfigurationError')
let CommandError = module.exports.CommandError = makeError('CommandError')

function Repository (options) {
  options = defaults(options, {
    model: {}
  })
  if (!options.path) {
    throw new ConfigurationError('Repository requires a path to a journalfile.\r\nUsage: require(\'R\')({path:\'j.log\'})')
  }

  // resolve journal path relative to root
  let journalPath = path.resolve(path.dirname(require.main.filename), options.path)
  debug('journalling to %s', journalPath)

  let model = options.model
  debug('initial model is %j', model)

  let lck = new RwLock()
  let isInitialized = false
  let commands = {}

  this.register = register
  this.query = query
  this.execute = execute

  function lock (type, fn) {
    return new Promise(function (resolve, reject) {
      lck[type](function (release) {
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

  function readLock (fn) {
    return lock('readLock', fn)
  }

  function writeLock (fn) {
    return lock('writeLock', fn)
  }

  function appendToJournal (line) {
    return co(function * () {
      try {
        return yield append()
      } catch (e) {
        if (e && e.code === 'ENOENT') {
          // the journal file doesnt exists nor can it be created
          // assume its cuz a missing folder
          debug('creating journalling folder')
          yield mkdirp(path.dirname(journalPath))
          return yield append()
        }
        throw e
      }
    })
    function append () {
      return fs.appendFile(journalPath, line + '\n', {
        encoding: 'utf8'
      })
    }
  }

  let marshalSkipTypes = {
    'undefined': true,
    'boolean': true,
    'number': true,
    'string': true
  }

  function marshal (value) {
    if ((value === null) || marshalSkipTypes[typeof value]) {
      return value
    }
    return typeof value.marshal === 'function' ? value.marshal() : JSON.parse(JSON.stringify(value))
  }

  function getCommand (name) {
    if (commands.hasOwnProperty(name)) {
      return commands[name]
    }
    throw new CommandError(util.format('Unkown command \'%s\'', name))
  }

  function init () {
    if (isInitialized) {
      return Promise.resolve(true)
    }
    return writeLock(function * () {
      if (isInitialized) {
        return true
      }

      debug('initializing')

      let replayCount = 0

      return new Promise(function (resolve, reject) {
        function onReadStreamError (err) {
          if (err && err.code === 'ENOENT') {
            debug('no journal to initialize from. skipping.')
            isInitialized = true
            return resolve(true)
          }
          reject(err)
        }

        fs.createReadStream(journalPath)
          .on('error', onReadStreamError)
          .pipe(new JsonStream())
          .on('error', reject)
          .pipe(costream.each(function * (record) {
            yield getCommand(record.n)(model, record.a, {model: model, name: record.n, arg: record.arg, replay: true})
            ++replayCount
          }, {
            objectMode: true,
            parallell: 1
          }))
          .on('error', reject)
          .on('finish', function () {
            isInitialized = true
            resolve(true)
          })
      })
      .then(function (res) {
        debug('initialization done. replayed commands: %s', replayCount)
        return res
      })
      .catch(function (err) {
        debug('initialization failed: %s', err)
        throw err
      })
    })
  }

  function register (name, executeFn) {
    commands[name] = executeFn
    return this
  }

  function query (queryFn) {
    return init()
      .then(function () {
        return readLock(function * () {
          let res = yield queryFn(model)
          return marshal(res)
        })
      })
  }

  function execute (name, arg) {
    let recordData = JSON.stringify({
      t: new Date(),
      n: name,
      a: arg
    })
    return init()
      .then(function () {
        return writeLock(function * () {
          yield appendToJournal(recordData)
          let res = yield getCommand(name)(model, arg, {model: model, name: name, arg: arg, replay: false})
          return marshal(res)
        })
      })
  }
}
