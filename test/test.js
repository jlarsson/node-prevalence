/* global describe, it, beforeEach, afterEach */

'use strict'

let assert = require('assert')
let co = require('co')
let prevalence = require('../index')
let fs = require('mz/fs')
let path = require('path')

describe('prevalence', function () {
  let journalPath = path.resolve(__dirname, '../tmp/testjournal.log')

  beforeEach(cleanRepo)
  afterEach(cleanRepo)

  describe('query(* -> yield)', function () {
    let repo = createRepo()

    co_it('with literal', function * () {
      let res = yield repo.query(function * () {
        return 123
      })
      assert.equal(123, res)
    })

    co_it('with promise', function * () {
      let res = yield repo.query(function * () {
        return Promise.resolve(123)
      })
      assert.equal(123, res)
    })

    co_it('can be run in parallell (readlock)', function * () {
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

    co_it('marshals the result', function * () {
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
    co_it('command must be registered', function * () {
      try {
        yield createRepo().execute('some unregistered command')
        assert.fail('expected throw')
      } catch (err) {
        assert.ok(err instanceof prevalence.CommandError)
      }
    })

    co_it('commands works (!)', function * () {
      let res = yield createRepo()
        .register('cmd', function * (model, arg, ctx) {
          return 123 * arg
        })
        .execute('cmd', 3)

      assert.equal(3 * 123, res)
    })

    co_it('marshals the result', function * () {
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

    co_it('are executed serially (writelock)', function * () {
      let maxActive = 0
      let active = 0

      let repo = createRepo({
          model: {
            counter: 0
          }
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

    co_it('invokations are logged to journal', function * () {
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
      for (let i = 0; i < 10; ++i) {
        yield repo.execute('inc-counter')
      }
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

  function co_it (description, gen) {
    return it(description, function (done) {
      co(gen)
        .then(done).catch(done)
    })
  }

  function createRepo (options) {
    (options || (options = {})).path = journalPath
    return prevalence(options)
  }

  function cleanRepo (done) {
    fs.unlink(journalPath)
      .then(done)
      .catch(function (err) {
        // silently ignore errors when cleaning up
        err = null
        done()
      })
  }
})
