const tape = require('tape')
const { Kappa } = require('..')
const { runAll } = require('./lib/util')
const createSimpleView = require('./lib/simple-view')
const createSimpleSource = require('./lib/simple-source')

tape('simple source', t => {
  const kappa = new Kappa()

  kappa.use('view1', createSimpleSource(), createSimpleView())
  kappa.use('view2', createSimpleSource(), createSimpleView())
  kappa.source.view1.push(1)
  kappa.source.view1.push(2)
  kappa.source.view2.push(3)
  kappa.source.view2.push(4)

  runAll([
    cb => kappa.view.view1.collect((err, res) => {
      t.error(err)
      t.deepEqual(res, [1, 2])
      cb()
    }),
    cb => kappa.view.view2.collect((err, res) => {
      t.error(err)
      t.deepEqual(res, [3, 4])
      cb()
    }),
    cb => t.end()
  ])
})

tape('finished handling', t => {
  const kappa = new Kappa()
  t.plan(5)

  let msgs = ['a', 'b', 'c']
  let i = 0
  kappa.use('foo', {
    pull (next) {
      let finished
      if (i !== msgs.length - 1) finished = false
      next(
        null,
        [msgs[i]],
        finished,
        function onindexed (cb) {
          t.pass('onindexed ' + i)
          i = i + 1
          cb()
        }
      )
    }
  }, createSimpleView())

  runAll([
    cb => kappa.view.foo.collect((err, res) => {
      t.error(err)
      t.deepEqual(res, ['a', 'b', 'c'])
      cb()
    }),
    cb => t.end()
  ])
})

tape('error on pull', t => {
  const kappa = new Kappa()
  let msgs = ['a']
  let i = 0
  kappa.use('foo', {
    pull (next) {
      if (i === 1) return next(new Error('pull error'))
      if (i > 1) t.fail('pull after error')
      next(
        null,
        msgs,
        false,
        function onindexed (cb) {
          t.pass('onindexed ' + i)
          i++
          cb()
        }
      )
    }
  }, createSimpleView())
  kappa.once('error', err => {
    t.equal(err.message, 'pull error')
    t.equal(kappa.flows.foo.state, 'error')
    t.end()
  })
})

tape('error on map', t => {
  const kappa = new Kappa()
  kappa.use('foo', createSimpleSource(), {
    map (messages, next) {
      next(new Error('map error'))
    }
  })
  kappa.source.foo.push('a')
  kappa.once('error', err => {
    t.equal(err.message, 'map error')
    t.equal(kappa.flows.foo.state, 'error')
    t.end()
  })
  kappa.ready(() => {
    t.fail('no ready on error')
  })
})

tape('state update', t => {
  const kappa = new Kappa()
  const foo = kappa.use('foo', createSimpleSource(), createSimpleView())
  let state
  foo.on('state-update', newState => {
    state = newState
  })
  foo.source.push([1, 2])
  process.nextTick(() => {
    foo.source.push([3, 4])
  })
  runAll([
    cb => setTimeout(cb, 0),
    cb => foo.view.collect((err, res) => {
      t.error(err, 'no error')
      t.deepEqual(res, [1, 2, 3, 4], 'result matches')
      t.deepEqual(state, {
        state: 'idle',
        totalBlocks: 4,
        indexedBlocks: 4,
        prevIndexedBlocks: 0
      }, 'state matches')
      t.deepEqual(state, foo.getState())
      cb()
    }),
    cb => {
      kappa.once('error', err => {
        t.equal(err.message, 'bad', 'error ok')
        process.nextTick(cb)
      })
      foo.source.error(new Error('bad'))
    },
    cb => {
      t.equal(state.state, 'error')
      t.equal(state.error.message, 'bad')
      t.equal(foo.getState().state, 'error')
      t.equal(foo.getState().error.message, 'bad')
      cb()
    },
    cb => t.end()
  ])
})

tape('reset', t => {
  const kappa = new Kappa()
  const foo = kappa.use('foo', createSimpleSource(), createSimpleView())
  foo.source.push(1)
  foo.source.push(2)
  foo.source.push(3)
  runAll([
    cb => foo.view.collect((err, res) => {
      t.error(err)
      t.deepEqual(res, [1, 2, 3])
      t.equal(kappa.view.foo.clearedCount(), 0)
      cb()
    }),
    cb => {
      kappa.reset('foo', cb)
    },
    cb => foo.view.collect((err, res) => {
      t.error(err)
      t.deepEqual(res, [1, 2, 3])
      t.equal(kappa.view.foo.clearedCount(), 1)
      cb()
    }),
    cb => t.end()
  ])
})

tape('open close', t => {
  t.plan(5)
  const kappa = new Kappa()
  let i = 0
  kappa.use('foo', {
    pull (next) {
      t.pass('pull')
      return next(
        null,
        [++i, ++i],
        true,
        function onindexed (cb) {
          t.pass('onindexed')
          cb()
        }
      )
    },
    open (flow, cb) {
      t.pass('open')
      cb()
    },
    close (cb) {
      t.pass('close')
      cb()
    }
  }, createSimpleView())

  runAll([
    cb => kappa.ready(cb),
    cb => kappa.close(cb),
    cb => {
      t.pass('closed!')
      cb()
    }
  ])
})

tape('open error', t => {
  const kappa = new Kappa()
  kappa.use('foo', {
    open (flow, cb) {
      cb(new Error('open error'))
    },
    pull (next) { next() }
  }, createSimpleView())
  kappa.use('bar', {
    open (flow, cb) {
      cb()
    },
    pull (next) { next() }
  }, createSimpleView())
  kappa.on('error', err => {
    t.equal(err.message, 'open error')
    t.equal(kappa.flows.foo.opened, false)
    t.equal(kappa.flows.bar.opened, true)
    t.end()
  })
})

tape('fetch version error', t => {
  const kappa = new Kappa()
  kappa.use('foo', {
    fetchVersion (cb) {
      cb(new Error('fetch version error'))
    },
    pull (next) { next() }
  }, createSimpleView())
  kappa.on('error', err => {
    t.equal(err.message, 'fetch version error')
    t.equal(kappa.flows.foo.opened, false)
    t.end()
  })
})
