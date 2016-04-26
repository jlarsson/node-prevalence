/* global describe, it, beforeEach, afterEach */
'use strict'

let assert = require('assert')
let prevalence = require('../index')
let fs = require('mz/fs')
let path = require('path')
let EventEmitter = require('events')
let Promise = require('any-promise')

require('co-mocha')

describe('prevalence', function () {
  let journalPath = path.resolve(__dirname, '../tmp/testjournal.log')

  beforeEach(cleanRepo)
  afterEach(cleanRepo)

  describe('query(* -> yield)', function () {
    let repo = createRepo()

    it('with literal', function * () {
      // This test shows that query() return values
      // can be simple JavaScript types
      let res = yield repo.query(function * () {
        return 123
      })
      assert.equal(123, res)
    })

    it('may return undefined', function * () {
      // This test shows that query() return values
      // can be simple JavaScript types
      let res = yield repo.query(function * () {
        return undefined
      })
      assert(res === undefined)
    })

    it('may return null', function * () {
      // This test shows that query() return values
      // can be simple JavaScript types
      let res = yield repo.query(function * () {
        return null
      })
      assert(res === null)
    })

    it('with promise', function * () {
      // This test shows that query() return values
      // can be promises/thenables, proving that
      // deferred/lazy evaluation is ok
      let res = yield repo.query(function * () {
        return Promise.resolve(123)
      })
      assert.equal(123, res)
    })

    it('can be run in parallell (readlock)', function * () {
      // This test shows that query() requires a shared read lock.
      // It is ok to have any number of parallel query()
      let maxActive = 0
      let active = 0
      yield [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      .map(function (i) {
        return repo.query(function * () {
          return new Promise(function (resolve) {
            ++active
            maxActive = Math.max(active, maxActive)
            setTimeout(function () {
              --active
              resolve(i)
            }, 30)
          })
        })
      })
      assert.equal(0, active)
      assert.equal(10, maxActive)
    })

    it('marshals the result', function * () {
      // This test shows that the result of query()
      // is a deep copy, allowing the sanboxed model
      // to remain isolated from the outside world
      let modelValue = {
        a: 123
      }
      let value = yield repo.query(function * () {
        return modelValue
      })
      assert.deepEqual(modelValue, value)
      assert.ok(modelValue !== value, 'Result should be a copy')
    })
  })

  describe('execute(* -> yield)', function () {
    it('command must be registered', function * () {
      // This test shows that commands must be registered.
      // This is since a command has both a name, and perhaps more important,
      // associated business logic.
      // In that sense, they are similar to SQL stored procedures.

      try {
        yield createRepo().execute('some unregistered command')
        assert.fail('expected throw')
      } catch (err) {
        assert.ok(err instanceof prevalence.CommandError)
      }
    })

    it('commands works (!)', function * () {
      let res = yield createRepo()
        .register('cmd', function * (model, arg, ctx) {
          return 123 * arg
        })
        .execute('cmd', 3)

      assert.equal(3 * 123, res)
    })

    it('may return null', function * () {
      let res = yield createRepo()
        .register('cmd', function * (model, arg, ctx) {
          return null
        })
        .execute('cmd')

      assert.equal(null, res)
    })

    it('marshals the result', function * () {
      // This test shows that the returnvalue from a command
      // is detached from the model.
      // The idea is that the model/data lives in a sandbox, and since its not
      // allowed to modify it ouside a command handler, it must be a deep copy
      // in order to prevent accidental violations to this rule.

      let modelValue = {
        a: 123
      }
      let value = yield createRepo()
        .register('cmd', function * () {
          return modelValue
        })
        .execute('cmd')
      assert.deepEqual(modelValue, value)
      assert.ok(modelValue !== value)
    })

    it('are executed serially (writelock)', function * () {
      // This test shows that execute() requires exclusive access.
      // In short, execute() takes a write lock, gaining exclusive access
      // amongst all other execute() and query()

      let maxActive = 0
      let active = 0

      let repo = createRepo({
        model: { counter: 0 }
      })
      .register('inc-counter', function * (model) {
        return new Promise(function (resolve) {
          ++active
          maxActive = Math.max(active, maxActive)
          setTimeout(function () {
            --active
            resolve(++model.counter)
          }, 30)
        })
      })

      yield [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      .map(function (i) {
        return repo.execute('inc-counter')
      })
      assert.equal(0, active)
      assert.equal(1, maxActive)
    })

    it('invokations are logged to journal', function * () {
      // This test simulates the case where a repo goes off line
      // for a while and then is restored to its last known state
      // using the journalled commands

      // Create repository with a registered command
      let repo = createRepo({
        model: {
          counter: 0
        }
      })
      .register('inc-counter', function * (model) {
        return ++model.counter
      })
      // Run command a couple of times
      yield [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      .map(function (i) {
        return repo.execute('inc-counter')
      })
      assert.equal(10, yield repo.query(function * (model) {
        return model.counter
      }))

      // Forget repo for a while
      repo = null
      // ...and then come back with full history
      let counter = yield createRepo({
        model: {
          counter: 0
        }
      })
      .register('inc-counter', function * (model) {
        return ++model.counter
      })
      .query(function * (model) {
        return model.counter
      })
      assert.equal(10, counter)
    })
  })

  describe('execute (<unregistered command> -> yield)', function () {
    it('throws CommandError by default', function * () {
      // This test shows that command must be registered.
      // This applies in particular to 'old' commands in the journal
      // For backwards compatibility its in general not safe to refactor
      // away commands, although their meaning may be refactored
      try {
        yield createRepo().execute('some unknown command')
        assert.fail('Expected CommandError')
      } catch (err) {
        assert.ok(err instanceof prevalence.CommandError)
      }
    })

    it('command \'*\' is fallback for all unmapped commands', function * () {
      // This test shows how one can register a catch-all command handler
      let result = yield createRepo()
        .register('*', function * (model) {
          return 'unmapped result'
        })
        .execute('some unknown command')
      assert.equal('unmapped result', result)
    })

    it('execute() emits "command" event', function * () {
      let d = deferred()
      let res = yield createRepo()
        .on('command', function (name, arg, result) {
          d.resolve({name: name, arg: arg, result: result})
        })
        .register('cmd', function * (model, arg, ctx) {
          return 123
        })
        .execute('cmd', 3)

      assert.equal(123, res)

      let emitted = yield d.promise
      assert(emitted)
      assert.equal(emitted.name, 'cmd')
      assert.equal(emitted.arg, 3)
      assert.equal(emitted.result, 123)
    })
  })

  it('execute() emits "error" event on failure', function * () {
    let d = deferred()
    try {
      yield createRepo()
        .on('error', d.resolve)
        .execute('some unknown command')
      assert.fail('Expected CommandError')
    } catch (err) {
      assert.ok(err instanceof prevalence.CommandError)
    }

    let emittedError = yield d.promise
    assert.ok(emittedError instanceof prevalence.CommandError)
  })

  describe('journal replay', function () {
    it('emits "init" event', function * () {
      let d = deferred()
      yield createRepo()
        .on('init', d.resolve)
        .query(function * () {})
      yield d.promise
    })

    it('initial exception is sticky (i.e. unrecoverable)', function * () {
      // The purpose of this test is to show that if the journal is corrupt or
      // the journal replay fails for some reason, that initial error is
      // reported back in all subsequent calls.
      // If the journal replay fails, it typically requires a manual fix of
      // some kind.

      var failCount = 0
      var failingJournal = {
        replay: function () {
          ++failCount
          throw new Error('journal error')
        }
      }

      var repo = createRepo({journal: failingJournal})

      var gotErrors = yield [0, 1, 2, 3].map(function () {
        return repo.query(function * () {})
          .catch(function (err) {
            assert.equal(err.message, 'journal error')
            return true
          })
      })

      assert.deepEqual([true, true, true, true], gotErrors)
      assert.equal(failCount, 1, 'The journal should only be invoked once, even when failing')
    })
  })

  function deferred () {
    // A tiny utility implementation of the deferred pattern
    let e = new EventEmitter()
    let p = new Promise(function (resolve, reject) {
      e.on('resolve', resolve).on('reject', reject)
    })
    return {
      promise: p,
      resolve: function (v) { e.emit('resolve', v) },
      reject: function (e) { e.emit('reject', e) }
    }
  }

  function createRepo (options) {
    (options || (options = {})).path = journalPath
    return prevalence(options)
  }

  function cleanRepo (done) {
    fs.unlink(journalPath)
      .then(done)
      .catch(function (err) {
        err = null // silently ignore cleanup errors
        done()
      })
  }
})
