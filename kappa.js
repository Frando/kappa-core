const thunky = require('thunky')
const { EventEmitter } = require('events')

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
    this._indexingState = {}

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
    this._state = new State()
  }

  get version () {
    return this._view.version
  }

  get status () {
    return this._state.state
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
      self._setState(Status.Ready)
      self.opened = true
      self.opening = false
      self._run()
      cb()
    }
  }

  close (cb) {
    if (!this.opened && !this.opening) return cb()
    if (!this.opened) return this.open(() => this.close(cb))
    const self = this
    // this.pause()
    let state = this._state.state
    this._setState(Status.Closing)

    if (state === Status.Running) return this.once('ready', close)
    else close()

    function close () {
      let pending = 1
      if (self._source.close) ++pending && self._source.close(done)
      if (self._view.close) ++pending && self._view.close(done)
      done()
      function done () {
        if (--pending !== 0) return
        self._setState(Status.Closed)
        self.opened = false
        cb()
      }
    }
  }

  ready (cb) {
    const self = this
    if (!this.opened) return this.open(() => this.ready(cb))

    setImmediate(() => {
      if (this._source.ready) this._source.ready(onsourceready)
      else onsourceready()
    })

    function onsourceready () {
      process.nextTick(() => {
        if (self._state.state === Status.Ready) process.nextTick(cb)
        else self.once('ready', cb)
      })
    }
  }

  pause () {
    this._setState(Status.Paused)
  }

  resume () {
    if (this._state.state !== Status.Paused) return
    if (!this.opened) return this.open()
    this._setState(Status.Ready)
    this._run()
  }

  reset (cb = noop) {
    const self = this
    const paused = this._state.state === Status.Paused
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
    if (!this.opened) return
    this.incomingUpdate = true
    process.nextTick(this._run.bind(this))
  }

  getState () {
    return this._state.get()
  }

  _setState (state, context) {
    this._state.set(state, context)
    this.emit('state-update', this._state.get())
    if (state === Status.Error) {
      this.emit('error', context && context.error)
    }
  }

  _run () {
    if (this._state.state !== Status.Ready) return
    const self = this

    this._setState(Status.Running)
    this._source.pull(onbatch)

    function onbatch (err, messages, finished, onindexed) {
      // If set to paused while pulling, drop the result and don't update state.
      if (self._state.state === Status.Paused) return

      if (err) return ondone(err)
      if (!messages) return ondone()
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
    }

    function ondone (err, messages, finished, onindexed) {
      if (err) return finish(err)
      if (messages && messages.length && self._view.indexed) {
        self._view.indexed(messages)
      }
      if (onindexed) {
        onindexed((err, context) => finish(err, finished, context))
      } else {
        finish(null, finished)
      }
    }

    function finish (err, finished = true, context) {
      if (err) {
        self._setState(Status.Error, { error: err })
      } else if (self._state.state === Status.Running) {
        self._setState(Status.Ready, context)
      }

      if (self._state.state === Status.Ready && (self.incomingUpdate || !finished)) {
        self.incomingUpdate = false
        process.nextTick(self._run.bind(self))
      } else {
        self.emit('ready')
      }
    }
  }
}

class State {
  constructor () {
    this.state = Status.Closed
    this.context = {}
  }

  set (state, context) {
    this.state = state
    if (context) this.context = { ...this.context, ...context }
  }

  get () {
    return Object.assign({ status: this.state }, this.context || {})
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
