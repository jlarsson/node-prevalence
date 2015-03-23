(function(module) {
  'use strict'

  var thenify = require('thenify'),
    co = require('co'),
    costream = require('co-stream'),
    debug = require('debug')('prevalence'),
    defaults = require('defaults'),
    fs = require('mz/fs'),
    jsonstream = require('json-stream'),
    makeError = require('make-error'),
    mkdirp = thenify(require('mkdirp')),
    path = require('path'),
    Promise = require('native-or-bluebird'),
    rwlock = require('rwlock'),
    util = require('util')

  module.exports = function(options) {
    return new Repository(options)
  }
  var ConfigurationError = module.exports.ConfigurationError = makeError('ConfigurationError')
  var CommandError = module.exports.CommandError = makeError('CommandError')

  function Repository(options) {
    options = defaults(options, {
      model: {}
    })
    if (!options.path) {
      throw new ConfigurationError('Repository requires a path to a journalfile.\r\nUsage: require(\'prevalence\')({path:<path to log>})')
    }

    // resolve journal path relative to root
    var journalPath = path.resolve(path.dirname(require.main.filename), options.path)
    debug('journalling to %s', journalPath)

    var model = options.model
    debug('initial model is %j', model)

    var isInitialized = false
    var commands = {}

    var lock = createLockObject()
    var marshal = createMarshalFunction()
    var appendToJournal = createJournalAppender(journalPath)

    // Register a match-unmapped-commands handler
    register('*', unregisteredCommandHandler)

    // Public methods
    this.register = register
    this.query = query
    this.execute = execute

    function register(name, executeFn) {
      commands[name] = executeFn
      return this
    }

    function getCommand(name) {
      if (commands.hasOwnProperty(name)) {
        return commands[name]
      }
      if (commands.hasOwnProperty('*')) {
        return commands['*']
      }

      return unregisteredCommandHandler
    }

    function query(queryFn) {
      return init()
        .then(function() {
          return lock.read(function*() {
            var res = yield queryFn(model)
            return marshal(res)
          })
        })
    }

    function execute(name, arg) {
      return init()
        .then(function() {
          return lock.write(function*() {
            var command = {
              model: model,
              name: name,
              arg: arg,
              replay: false
            }
            yield appendToJournal(command)
            var res = yield exec(command)
            return marshal(res)
          })
        })
    }

    function init() {
      if (isInitialized) {
        return Promise.resolve(true)
      }
      return lock.write(function*() {
        if (isInitialized) {
          return Promise.resolve(true)
        }
        debug('initializing')
        var replayCount = 0

        return yield createJournalReader(journalPath, replayExec.bind(null, model, exec))
          .then(function() {
            isInitialized = true
            debug('initialization done. replayed commands: %s', replayCount)
          })
          .catch(function(err) {
            debug('initialization failed: %s', err)
            throw err
          })

        function replayExec(model, exec, command) {
          ++replayCount
          // Fill in model since its not persisted in journal (only name and arg)
          command.model = model
          return exec(command)
        }
      })
    }

    function exec(command) {
      return getCommand(command.name)(command.model, command.arg, command)
    }

    // Readers/Writers lock factory
    function createLockObject() {
      var lck = new rwlock()
      return {
        read: lock.bind(null, 'readLock'),
        write: lock.bind(null, 'writeLock')
      }

      function lock(type, gen) {
        return new Promise(function(resolve, reject) {
          lck[type](function(release) {
            co(gen)
              .then(function(res) {
                resolve(res)
                release()
              })
              .catch(function(err) {
                reject(err)
                release()
              })
          })
        })
      }
    }

    // The purpose of a marshaller is to create a deep copy, so that
    // modifications to values returned from execute()/read() doesnt jeopardize
    // the stability/correctness of the managed prevalent model.
    function createMarshalFunction() {
      var dontMarshalTheseScalarTypes = {
        "undefined": true,
        "boolean": true,
        "number": true,
        "string": true,
        "function": true
      }
      return function marshal(skip, value) {
        if ((value === null) || skip[typeof(value)]) {
          return value
        }
        return typeof(value.marshal) === 'function' ? value.marshal() : JSON.parse(JSON.stringify(value))
      }.bind(null, dontMarshalTheseScalarTypes)
    }

    // A journal appender appends a json snapshot of command characteristics
    function createJournalAppender(journalPath) {
      return function appendToJournal(journalPath, command) {
        var line = JSON.stringify({
          t: new Date(),
          n: command.name,
          a: command.arg
        }) + '\n'

        return co(function*() {
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

        function append() {
          return fs.appendFile(journalPath, line, {
            encoding: 'utf8'
          })
        }
      }.bind(null, journalPath)
    }


    // A journal reader works as
    // - for each line
    // - parse line as json
    // - then extract command characteristics (name, argument)
    // - then execute the command
    function createJournalReader(journalPath, exec) {
      return new Promise(function(resolve, reject) {
        return fs.createReadStream(journalPath).on('error', onReadStreamError)
          .pipe(new jsonstream()).on('error', reject)
          .pipe(costream.each(function*(record) {
            yield exec({
              name: record.n,
              arg: record.a,
              replay: true
            })
          }, {
            objectMode: true,
            parallell: 1
          }))
          .on('error', reject)
          .on('finish', function() {
            resolve(true)
          })

        function onReadStreamError(err) {
          if (err && err.code == 'ENOENT') {
            // A missing journal file is equivalent to an empty one
            return resolve(true)
          }
          reject(err)
        }
      })
    }

    // Handle unregistered commands by throwing
    function* unregisteredCommandHandler(model, arg, ctx) {
      throw new CommandError(util.format('Unregistered command \'%s\'.\nFix: var repo = require(\'prevalence\').register(\'%s\', function *(){ ... })', ctx.name, ctx.name))
    }

  }
})(module)
