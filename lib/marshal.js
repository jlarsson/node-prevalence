'use strict'

module.exports = marshal

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
