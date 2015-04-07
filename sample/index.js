'use strict'

let path = require('path')
let co = require('co')
let prevalence = require('../')

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
