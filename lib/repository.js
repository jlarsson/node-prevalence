'use strict'

let co = require('co')
let debug = require('debug')('prevalence')
let defaults = require('defaults')
let path = require('path')
let util = require('util')
let makeError = require('make-error')

let lock = require('./lock')
let marshal = require('./marshal')
let journal = require('./journal')

module.exports = function (options) {
  return new Repository(options)
}
module.exports.Repository = Repository

let ConfigurationError = module.exports.ConfigurationError = makeError('ConfigurationError')
let CommandError = module.exports.CommandError = makeError('CommandError')

function Repository (options) {
  options = defaults(options, {
    model: {}
  })
  this.initResult = null
  this.model = options.model
  this.marshal = options.marshal || marshal
  this.lock = options.lock || lock()
  this.journal = options.journal
  this.commands = {}

  if (!this.journal) {
    if (!options.path) {
      throw new ConfigurationError('Repository requires a path to a journalfile.\r\nUsage: require(\'R\')({path:\'j.log\'})')
    }
    let journalPath = path.resolve(path.dirname(require.main.filename), options.path)
    debug('journalling to %s', journalPath)
    this.journal = journal(journalPath)
  }

  debug('initial model is %j', this.model)
}

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
        let res = yield queryFn(this.model)
        return this.marshal(res)
      }.bind(this))
      .then(function (r) {
        return r
      })
    }.bind(this))
}

proto.execute = function Repository$execute (name, arg) {
  let recordData = JSON.stringify({
    t: new Date(),
    n: name,
    a: arg
  })
  return this.init()
    .then(function () {
      return this.writeLock(function * () {
        yield this.journal.append(recordData)
        let result = yield this.getCommand(name)(this.model, arg, {model: this.model, name: name, arg: arg, replay: false})
        return this.marshal(result)
      }.bind(this))
    }.bind(this))
}

function executeReplayCommand (record) {
  return this.getCommand(record.n)(this.model, record.a, {model: this.model, name: record.n, arg: record.arg, replay: true})
}

proto.init = function Repository$init () {
  if (!this.initResult) {
    this.initResult = co(function * () {
      this.writeLock(function * () {
        yield this.journal.replay(executeReplayCommand.bind(this))
        this.initialized = true
      }.bind(this))
    }.bind(this))
  }
  return this.initResult
}
