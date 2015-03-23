var assert = require('assert'),
  co = require('co'),
  fs = require('mz/fs'),
  path = require('path'),
  prevalence = require('../index')

describe('prevalence', function() {
  var journalPath = path.resolve(__dirname, '../tmp/testjournal.log')

  beforeEach(cleanRepo)
  afterEach(cleanRepo)

  describe('query(* -> yield)', function() {
    var repo = createRepo()

    co_it('with literal', function*() {
      var res = yield repo.query(function*() {
        return 123
      })
      assert.equal(123, res)
    })

    co_it('with promise', function*() {
      var res = yield repo.query(function*() {
        return Promise.resolve(123)
      })
      assert.equal(123, res)
    })

    co_it('can be run in parallell (readlock)', function*() {
      var maxActive = 0
      var active = 0
      yield [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      .map(function(i) {
        return repo.query(function*() {
          return new Promise(function(resolve) {
            ++active
            maxActive = Math.max(active, maxActive)
            setTimeout(function() {
              --active
              resolve(i)
            }, 30)
          })
        })
      })
      assert.equal(0, active)
      assert.equal(10, maxActive)
    })

    co_it('marshals the result', function*() {
      var modelValue = {
        a: 123
      }
      var value = yield repo.query(function*() {
        return modelValue
      })
      assert.deepEqual(modelValue, value, "Expected structural equivalence")
      assert.ok(modelValue !== value, "Should be a copy and not same instance")
    })
  })

  describe('execute(* -> yield)', function() {

    co_it('command must be registered', function*() {
      try {
        yield createRepo().execute('some unregistered command')
        assert.fail('illegal code, expected throw on previous line due to execute of unregistered command')
      } catch (err) {
        assert.ok(err instanceof prevalence.CommandError, "Expcted CommandError")
      }
    })

    co_it('commands works (!)', function*() {
      var res = yield createRepo()
        .register('cmd', function*(model, arg, ctx) {
          return 123 * arg
        })
        .execute('cmd', 3)

      assert.equal(3 * 123, res)
    })

    co_it('marshals the result', function*() {
      var modelValue = {
        a: 123
      }
      var value = yield createRepo()
        .register('cmd', function*() {
          return modelValue
        })
        .execute('cmd')
      assert.deepEqual(modelValue, value, "Expected structural equivalence")
      assert.ok(modelValue !== value, "Should be a copy and not same instance")
    })

    co_it('are executed serially (writelock)', function*() {
      var maxActive = 0
      var active = 0

      var repo = createRepo({
          model: {
            counter: 0
          }
        })
        .register('inc-counter', function*(model) {
          return new Promise(function(resolve) {
            ++active
            maxActive = Math.max(active, maxActive)
            setTimeout(function() {
              --active
              resolve(++model.counter)
            }, 30)
          })
        })

      yield [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      .map(function(i) {
        return repo.execute('inc-counter')
      })
      assert.equal(0, active)
      assert.equal(1, maxActive)
    })

    co_it('invokations are logged to/replayed from journal', function*() {
      function connect() {
        return createRepo({
            model: {
              counter: 0
            }
          })
          .register('inc-counter', function*(model) {
            return ++model.counter
          })
      }

      // Create repository with a registered command
      var repo = connect()
        // Run command a couple of times
      for (var i = 0; i < 10; ++i) {
        yield repo.execute('inc-counter')
      }
      assert.equal(10, yield repo.query(function*(model) {
        return model.counter
      }))

      // Forget repo for a while
      repo = null
        // ...and then come back with full history
      var counter = yield connect()
        .query(function*(model) {
          return model.counter
        })
      assert.equal(10, counter)
    })

    co_it('command effects are persisted between sessions', function*() {
      function connect() {
        return createRepo({
            model: {
              counter: 0
            }
          })
          .register('inc-counter', function*(model) {
            return ++model.counter
          })
      }

      // Connect, execute and detach some times
      for (var i = 0; i < 10; ++i) {
        yield connect().execute('inc-counter')
      }
      // Connect again and check counter
      var counter = yield connect().query(function*(model) {
        return model.counter
      })
      assert.equal(10, counter)
    })

    co_it('command arguments are persisted between sessions', function*() {
      function connect() {
        return createRepo({
            model: {
              name: ''
            }
          })
          .register('set-name', function*(model, name) {
            return model.name = name
          })
      }
      yield connect().execute('set-name', 'testing')

      var name = yield connect().query(function*(model) {
        return model.name
      })
      assert.equal('testing', name)
    })
  })

  function co_it(description, gen) {
    return it(description, function(done) {
      co(gen).then(done).catch(done)
    })
  }

  function createRepo(options) {
    (options || (options = {})).path = journalPath
    return prevalence(options)
  }

  function cleanRepo(done) {
    fs.unlink(journalPath)
      .then(done)
      .catch(done.bind(null, null))
  }
})
