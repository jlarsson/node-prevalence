# node-prevalence

[![npm][npm-image]][npm-url]
[![travis][travis-image]][travis-url]
[![license][license-image]][license-url]
[![js-standard-style][standard-image]](standard-url)

Promise based, co/koa-friendly prevalence component.

Think of it a super extensible, super fast, in-memory database that will restore it self from a history of commands.

## Sample code
The sample code below two blog posts and then prints them, illustrating
- command registration via ```register()```
- command execution via ```execute()```
- querying via ```query()```

```javascript
'use strict'

let path = require('path')
let co = require('co')
let prevalence = require('prevalence')

let repo = prevalence({
  path: path.resolve(__dirname, '../tmp/sample.journal'),
  model: {
    posts: {}
  }
})
.register('add-post', function * (model, post) {
  model.posts[post.id] = post
  return post
})

var log = console.log

co(function * () {
  yield repo.execute('add-post', {id: 'post#1', subject: 'Lorem'})
  yield repo.execute('add-post', {id: 'post#2', subject: 'Ipsum'})

  let posts = yield repo.query(function * (model) {
    let posts = []
    for (let id of Object.getOwnPropertyNames(model.posts)) {
      posts.push(model.posts[id])
    }
    return posts
  })

  for (let post of posts){
    log('%s: %s', post.id, post.subject)
  }
})
```

## Overview

Prevalence acts as a simple in-memory database with very controlled access patterns.

- Queries may inspect the data model (in parallel controlled by a read lock)
- Commands may alter the data model (serially controlled by a write lock)
- Commands must be described (with name and a handler) and invocations are logged with name and argument to a durable history (journal) so that the command can be re-executed on startup, thus restoring the model to the last known state.
- Results from queries and commands are by default serialized (i.e. deep copied), eliminating some nasty race conditions.


Check out [wikipedia](http://en.wikipedia.org/wiki/System_Prevalence) for a more formal definition of the prevalence pattern. Note that this implementation deviates on some key points. Most notably the support for snapshots is dropped.


[travis-image]: https://img.shields.io/travis/jlarsson/node-prevalence.svg?style=flat
[travis-url]: https://travis-ci.org/jlarsson/node-prevalence
[npm-image]: https://img.shields.io/npm/v/prevalence.svg?style=flat
[npm-url]: https://npmjs.org/package/prevalence
[license-image]: https://img.shields.io/npm/l/prevalence.svg?style=flat
[license-url]: LICENSE.md
[standard-image]: https://img.shields.io/badge/code%20style-standard-brightgreen.svg?style=flat
[standard-url]: https://github.com/feross/standard
