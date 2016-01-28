'use strict'

let debug = require('debug')('prevalence')
let defaults = require('defaults')
let EventEmitter = require('events')
let path = require('path')
let util = require('util')
let makeError = require('make-error')

let lock = require('./lock')
let marshal = require('./marshal')
let journal = require('./journal')

module.exports = Repository

let ConfigurationError = module.exports.ConfigurationError = makeError('ConfigurationError')
let CommandError = module.exports.CommandError = makeError('CommandError')

function Repository (options) {
  if (!(this instanceof Repository)) {
    return new Repository(options)
  }

  EventEmitter.call(this)

  options = defaults(options, {
    model: {}
  })
  this.initResult = null
  this.model = options.model
  this.marshal = options.marshal || marshal
  this.lock = options.lock || lock()
  this.journal = options.journal
  this.commands = {}

  this._onError = function (err) {
    this.emit('error', err)
    throw err
  }.bind(this)

  if (!this.journal) {
    if (!options.path) {
      throw new ConfigurationError('Repository requires a path to a journalfile.\r\nUsage: require(\'prevalence\')({path:\'journal.log\'})')
    }
    let journalPath = path.resolve(path.dirname(require.main.filename), options.path)
    debug('journalling to %s', journalPath)
    this.journal = journal(journalPath)
  }

  debug('initial model is %j', this.model)
}

util.inherits(Repository, EventEmitter)

let proto = Repository.prototype

proto.register = function Repository$register (name, executeFn) {
  this.commands[name] = executeFn
  return this
}

proto.getCommand = function Repository$getCommand (name) {
  if (this.commands.hasOwnProperty(name)) {
    return this.commands[name]
  }
  if (this.commands.hasOwnProperty('*')) {
    return this.commands['*']
  }
  throw new CommandError(util.format('Unknown command \'%s\'', name))
}

proto.readLock = function Repository$readLock (fn) { return this.lock.readLock(fn) }

proto.writeLock = function Repository$writeLock (fn) { return this.lock.writeLock(fn) }

proto.query = function Repository$query (queryFn) {
  return this.init()
    .then(function () {
      return this.readLock(function * () {
        let result = yield queryFn(this.model)
        return this.marshal(result)
      }.bind(this))
    }.bind(this))
    .catch(this._onError)
}

proto.execute = function Repository$execute (name, arg) {
  let journalData = JSON.stringify({
    t: new Date(),
    n: name,
    a: arg
  })
  return this.init()
    .then(function () {
      return this.writeLock(function * () {
        yield this.journal.append(journalData)
        let result = yield this.getCommand(name)(this.model, arg, {model: this.model, name: name, arg: arg, replay: false})
        return this.marshal(result)
      }.bind(this))
    }.bind(this))
    .then(function (v) {
      this.emit('command', name, arg, v)
      return v
    }.bind(this))
    .catch(this._onError)
}

proto.init = function Repository$init () {
  if (!this.initResult) {
    this.initResult = this.writeLock(function * () {
      yield this.journal.replay(executeReplayCommand.bind(this))
      this.initialized = true
    }.bind(this))
    .then(function (v) {
      this.emit('init')
      return v
    }.bind(this))
  }
  return this.initResult
}

function executeReplayCommand (record) {
  return this.getCommand(record.n)(this.model, record.a, {model: this.model, name: record.n, arg: record.arg, replay: true})
}
