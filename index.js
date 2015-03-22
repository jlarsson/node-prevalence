'use strict';

var Promise = require('native-or-bluebird');
var co = require('co');
var thenify = require('thenify');
var costream = require('co-stream')
var debug = require('debug')('prevalence');
var rwlock = require('rwlock');
var defaults = require('defaults');
var jsonstream = require('json-stream')
var fs = require('mz/fs');
var path = require('path');
var mkdirp = thenify(require('mkdirp'));
var util = require('util');
var makeError = require('make-error');

module.exports = function(options) {
  return new Repository(options);
}

var ConfigurationError = module.exports.ConfigurationError = makeError('ConfigurationError');
var CommandError = module.exports.CommandError = makeError('CommandError');

function Repository(options) {
  options = defaults(options, {
    model: {}
  });
  if (!options.path){
    throw new ConfigurationError('Repository requires a path to a journalfile.\r\nUsage: require(\'R\')({path:\'j.log\'})')
  }

  // resolve journal path relative to root
  var journalPath = path.resolve( path.dirname(require.main.filename),options.path);
  debug('journalling to %s', journalPath);

  var model = options.model;
  debug('initial model is %j', model);

  var lck = new rwlock();
  var isInitialized = false;
  var commands = {};

  function lock(type, fn) {
    return new Promise(function(resolve, reject) {
      //debug('%s acquire...', type);
      lck[type](function(release) {
        //debug('%s acquired', type);
        co(fn)
          .then(function(res) {
            resolve(res);
            release();
            //debug('%s released', type);
          })
          .catch(function(err) {
            reject(err);
            release();
            //debug('%s released: %s', type, err);
          })
      });
    })
  }

  function readLock(fn) {
    return lock('readLock', fn);
  }

  function writeLock(fn) {
    return lock('writeLock', fn);
  }
/*
  var pendingJournal = [];
  function appendToJournal(line) {
    pendingJournal.push(line);
    if (pendingJournal.length === 1){
      requestFlush();
    }
    return Promise.resolve(true);


    function flush(){
      co(function*() {
        debug('flushing to journal: %s entries', pendingJournal.length);
        pendingJournal.push('');
        var lines = pendingJournal.join('\r\n');
        try {
          return yield fs.appendFile(journalPath, lines, {
            encoding: 'utf8'
          });
        } catch (e) {
          if (e && e.code === 'ENOENT') {
            // the journal file doesnt exists nor can it be created
            // assume its cuz a missing folder
            debug('creating journalling folder');
            yield mkdirp(path.dirname(journalPath));
            return yield fs.appendFile(journalPath, lines, {
              encoding: 'utf8'
            });
          }
          throw e;
        }
      })
      .then(function (){
        pendingJournal = [];
        debug('flushing done');
      })
      .then(requestFlush)
    }

    function requestFlush(){
      if (pendingJournal.length > 0){
        setImmediate(flush);
      }
    }
  }
*/
  function appendToJournal(line) {
    return co(function*() {
      try {
        return yield append();
      } catch (e) {
        if (e && e.code === 'ENOENT') {
          // the journal file doesnt exists nor can it be created
          // assume its cuz a missing folder
          debug('creating journalling folder');
          yield mkdirp(path.dirname(journalPath));
          return yield append();
        }
        throw e;
      }
    });
    function append(){
      return fs.appendFile(journalPath, line + '\n', {
        encoding: 'utf8'
      });
    }
  }

  var marshalSkipTypes = {
    "undefined": true,
    "boolean": true,
    "number": true,
    "string": true
  };

  function marshal(value) {
    if ((value === null) || marshalSkipTypes[typeof(value)]) {
      return value;
    }
    return typeof(value.marshal) === 'function' ? value.marshal() : JSON.parse(JSON.stringify(value));
  }

  function getCommand(name){
    if (commands.hasOwnProperty(name)){
     return commands[name];}
    throw new CommandError(util.format('Unkown command \'%s\'', name));
  }

  function init() {
    if (isInitialized) {
      return Promise.resolve(true);
    }
    return writeLock(function*() {
      if (isInitialized) {
        return true;
      }

      debug('initializing');

      var replayCount = 0;

      return new Promise(function(resolve, reject) {
        function onReadStreamError(err) {
          if (err && err.code == 'ENOENT') {
            debug('no journal to initialize from. skipping.')
            isInitialized = true;
            return resolve(true);
          }
          reject(err);
        }

        fs.createReadStream(journalPath)
          .on('error', onReadStreamError)
          .pipe(new jsonstream())
          .on('error', reject)
          .pipe(costream.each(function*(record) {
            yield getCommand(record.n)(model, record.a, {model: model, name: record.n, arg: record.arg, replay: true});
            ++replayCount;
          }, {
            objectMode: true,
            parallell: 1
          }))
          .on('error', reject)
          .on('finish', function() {
            isInitialized = true;
            resolve(true);
          })
      })
      .then(function (res){
        debug('initialization done. replayed commands: %s', replayCount);
        return res;
      })
      .catch(function (err){
        debug('initialization failed: %s', err);
        throw err;
      });
    });
  }

  function register(name, executeFn) {
    commands[name] = executeFn;
    return this;
  }

  function query(queryFn) {
    return init()
      .then(function() {
        return readLock(function*() {
          var res = yield queryFn(model);
          return marshal(res);
        });
      });
  }

  function execute(name, arg) {
    var recordData = JSON.stringify({
      t: new Date(),
      n: name,
      a: arg
    });
    return init()
      .then(function() {
        return writeLock(function*() {
          yield appendToJournal(recordData);
          var res = yield getCommand(name)(model, arg, {model: model, name: name, arg: arg, replay: false});
          return marshal(res);
        })
      });
  }

  this.register = register;
  this.query = query;
  this.execute = execute;
}
