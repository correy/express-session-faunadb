
'use strict';;;


import FlakeId from 'flake-idgen'
import xxhash from 'js-xxhash'
const {xxHash32} = xxhash
import intFormat from 'biguint-format'


const noop = _ => _


export function createIDGen ({datacenter, worker, datacenterID, workerID}) {
  const generator = new FlakeId({
    datacenter: datacenter || xxHash32(Buffer.from(datacenterID, 'utf8')) % 32,
    worker: worker || xxHash32(Buffer.from(workerID, 'utf8')) % 32,
  })

  return function genid () {
    return intFormat(generator.next(), 'dec')
  }
}


export function createStore (expressSession, faunadb) {
  const q = faunadb.query

  return class FaunaDBStore extends expressSession.Store {
    constructor (db, col) {
      if (!db) throw 'pass a fauna client'
      if (!col) throw 'pass a collection name'

      super()

      this.db = db
      this.col = q.Collection(col)
    }

    set (id, data, cb = noop) {
      const ref = q.Ref(this.col, id)

      this.db
        .query(
          q.If(
            q.Exists(ref),
            q.Update(ref, {data}),
            q.Create(ref, {data})
          )
        )
        .then(_ => cb(null))
        .catch(cb)
    }

    get (id, cb = noop) {
      this.db
        .query( q.Get(q.Ref(this.col, id)) )
        .then(({data}) => cb(null, data))
        .catch(cb)
    }

    touch (id, data, cb) {
      this.set(id, data, cb)
    }

    destroy (id, cb = noop) {
      this.db
        .query( q.Delete(q.Ref(this.col, id)) )
        .then(_ => cb(null))
        .catch(cb)
    }
  }
}
