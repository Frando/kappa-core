const thunky = require('thunky')
const { EventEmitter } = require('events')
const Runner = require('./lib/runner')

const Status = {
  Closed: 'closed',
  Ready: 'ready',
  Running: 'running',
  Paused: 'paused',
  Closing: 'closing',
  Error: 'error'
}

module.exports = class Kappa extends EventEmitter {
  /**
   * Create a kappa core.
   * @constructor
   */
  constructor (opts = {}) {
    super()
    this.flows = {}
    this.status = Status.Ready
    // APIs
    this.view = {}
    this.source = {}
  }

  // This is here for backwards compatibility.
  get api () { return this.view }

  use (name, source, view, opts = {}) {
    opts.status = opts.status || this.status
    const flow = new Flow(name, source, view, opts)
    this.flows[name] = flow
    this.view[name] = flow.view
    this.source[name] = flow.source

    flow.on('error', err => this.emit('error', err, flow))
    flow.on('state-update', state => this.emit('state-update', name, state))

    if (this.status !== Status.Paused) {
      process.nextTick(() => flow.open(err => {
        if (err) this.emit('error', err)
      }))
    }

    this.emit('flow', name)
    return flow
  }

  pause () {
    this.status = Status.Paused
    this._forEach(flow => flow.pause())
  }

  resume () {
    if (this.status !== Status.Paused) return
    this._forEach(flow => flow.resume())
    this.status = Status.Ready
  }

  reset (names, cb) {
    this._forEachAsync((flow, next) => {
      flow.reset(next)
    }, names, cb)
  }

  getState () {
    const state = {}
    this._forEach(flow => (state[flow.name] = flow.getState()))
    return state
  }

  ready (names, cb) {
    this._forEachAsync((flow, next) => {
      flow.ready(next)
    }, names, cb)
  }

  close (cb) {
    this.status = Status.Closing
    process.nextTick(() => {
      this._forEachAsync((flow, next) => {
        flow.close(next)
      }, err => {
        this.status = Status.Closed
        cb(err)
      })
    })
  }

  _forEach (fn, names) {
    if (typeof names === 'string') names = [names]
    if (!names) names = Object.keys(this.flows)
    for (const name of names) {
      if (!this.flows[name]) continue
      fn(this.flows[name])
    }
  }

  _forEachAsync (fn, names, cb) {
    if (typeof names === 'function') {
      cb = names
      names = null
    }
    cb = once(cb)
    let pending = 1
    this._forEach(flow => {
      ++pending
      fn(flow, done)
    }, names)
    done()
    function done (err) {
      if (err) return cb(err)
      if (--pending === 0) cb()
    }
  }
}

class Flow extends EventEmitter {
  constructor (name, source, view, opts) {
    super()

    this.opts = opts
    this.name = name

    if (!view.api) view.api = {}
    if (!source.api) source.api = {}
    if (!view.version) view.version = 1

    // TODO: Backward-compatibility only. Remove.
    if (view.clearIndex && !view.reset) {
      view.reset = view.clearIndex.bind(view)
    }

    if (view.api.ready === undefined) {
      view.api.ready = cb => this.ready(cb)
    }

    this._view = view
    this._source = source

    this._context = opts.context

    // Assign view and source apis
    this.view = this._view.api
    this.source = this._source.api

    // Create the list of funtions through which messages run between pull and map.
    this._transform = new Pipeline()
    if (this._source.transform) this._transform.push(this._source.transform.bind(this._source))
    if (this.opts.transform) this._transform.push(this.opts.transform)
    if (this._view.transform) this._transform.push(this._view.transform.bind(this._view))

    this.opened = false
    this.open = thunky(this._open.bind(this))

    this._runner = new Runner(this._work.bind(this))
    this._runner.on('ready', () => this.emit('ready'))
    this._runner.on('error', (err) => this.emit('error', err))
    this._runner.on('state', (state) => {
      this.emit('state-update', { ...this.getState(), state })
    })
    this._progress = {}
  }

  get version () {
    return this._view.version
  }

  get status () {
    return this._state.state
  }

  get state () {
    return this._runner.state
  }

  _open (cb = noop) {
    if (this.opened) return cb()
    const self = this
    this.opening = true
    let done = false
    let pending = 1
    if (this._view.open) ++pending && this._view.open(this, onopen)
    if (this._source.open) ++pending && this._source.open(this, onopen)
    onopen()

    function onopen (err) {
      if (err) return ondone(err)
      if (--pending !== 0) return
      if (!self._source.fetchVersion) return ondone()

      self._source.fetchVersion((err, version) => {
        if (err) return ondone(err)
        if (!version) {
          self._source.storeVersion(self.version, ondone)
        } else if (version !== self.version) {
          self.reset(() => {
            self._source.storeVersion(self.version, ondone)
          })
        } else {
          ondone()
        }
      })
    }

    function ondone (err) {
      if (done) return
      done = true
      if (err) return cb(err)
      self.opened = true
      self.opening = false
      self._runner.start()
      cb()
    }
  }

  close (cb) {
    if (!this.opened && !this.opening) return cb()
    if (!this.opened) return this.open(() => this.close(cb))
    const self = this

    this._runner.stop(close)

    function close () {
      let pending = 1
      if (self._source.close) ++pending && self._source.close(done)
      if (self._view.close) ++pending && self._view.close(done)
      done()
      function done () {
        if (--pending !== 0) return
        self.closed = true
        self.opened = false
        cb()
      }
    }
  }

  ready (cb) {
    const self = this
    if (this.closed) return cb()
    if (!this.opened) return this.open(() => this.ready(cb))

    setImmediate(() => {
      if (this._source.ready) this._source.ready(onsourceready)
      else onsourceready()
    })

    function onsourceready () {
      process.nextTick(() => {
        self._runner.ready(cb)
      })
    }
  }

  pause () {
    this._runner.pause()
  }

  resume () {
    this._runner.resume()
  }

  reset (cb = noop) {
    const self = this
    const paused = this.state === 'paused'
    this.pause()
    let pending = 1
    process.nextTick(() => {
      if (this._view.reset) ++pending && this._view.reset(done)
      if (this._source.reset) ++pending && this._source.reset(done)
      done()
    })
    function done () {
      if (--pending !== 0) return
      if (!paused) self.resume()
      cb()
    }
  }

  update () {
    this._runner.update()
  }

  getState () {
    const status = { state: this.state, ...this._progress }
    if (this.state === 'error') status.error = this._runner._error
    return status
  }

  _setProgress (progress) {
    if (!progress) return
    this._progress = { ...this._progress, ...progress }
  }

  _work (cb) {
    const self = this
    this._source.pull((err, messages, finished, onindexed) => {
      if (err) return ondone(err)
      if (!messages) messages = []
      messages = messages.filter(m => m)
      if (!messages.length) return ondone()
      self._transform.run(messages, messages => {
        if (!messages.length) return ondone()
        // TODO: Handle timeout?
        const callback = function (err) {
          ondone(err, messages, finished, onindexed)
        }
        const maybePromise = self._view.map(messages, callback)
        if (maybePromise && maybePromise.then) {
          maybePromise.then(() => callback()).catch(callback)
        }
      })
    })

    function ondone (err, messages, finished, onindexed) {
      if (err) return cb(err)
      if (messages && messages.length && self._view.indexed) {
        self._view.indexed(messages)
      }
      if (onindexed) {
        onindexed((err, context) => finish(err, finished, context))
      } else {
        finish(null, finished)
      }

      function finish (err, finished, context) {
        self._setProgress(context)
        if (finished === false) self._runner.update()
        cb(err)
      }
    }
  }
}

// Utils

class Pipeline {
  constructor () {
    this.fns = []
  }

  push (fn) {
    this.fns.push(fn)
  }

  run (messages, final) {
    runThrough(messages, this.fns, final)
  }
}

function runThrough (state, fns, final) {
  fns = fns.filter(f => f)
  next(state)
  function next (state) {
    const fn = fns.shift()
    if (!fn) return final(state)
    fn(state, nextState => {
      process.nextTick(next, nextState)
    })
  }
}

function noop () {}

function once (fn) {
  let called = false
  return (...args) => {
    if (called) return
    called = true
    fn(...args)
  }
}
