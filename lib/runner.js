const { EventEmitter } = require('events')

module.exports = class Runner extends EventEmitter {
  constructor (work) {
    super()
    this._ondone = this._ondone.bind(this)
    this._work = work
    this._state = 'idle'

    Object.defineProperty(this, 'state', {
      set: (value) => {
        this._state = value
        this.emit('state', value)
      },
      get: () => {
        return this._state
      }
    })
  }

  start () {
    this.state = 'idle'
    this._hasWork = true
    this._queueRun()
  }

  stop (cb) {
    this._closing = true
    if (this.state === 'idle') {
      this.state = 'closed'
      process.nextTick(cb)
    } else {
      this.once('_closed', cb)
    }
  }

  pause () {
    this.state = 'paused'
  }

  resume () {
    this.state = 'idle'
    this._queueRun()
  }

  update () {
    this._queueRun()
  }

  ready (cb) {
    if (this.state !== 'running' && this.state !== 'error') process.nextTick(cb)
    else this.once('ready', cb)
  }

  _run () {
    if (this.state !== 'idle') return
    if (this._hasWork === false) return

    this._runIsQueued = false
    this.state = 'running'

    this._work(this._ondone)
  }

  _ondone (err) {
    if (err) {
      this._error = err
      this.state = 'error'
      this.emit('error', err)
      return
    }

    if (this._closing) {
      this.state = 'closed'
      this.emit('_closed')
      return
    }

    if (this.state !== 'paused') {
      this.state = 'idle'
    }

    this.emit('ready')
  }

  _queueRun () {
    this._hasWork = true
    if (this._runIsQueued) return
    this._runIsQueued = true
    if (this.state === 'idle') {
      process.nextTick(() => this._run())
    } else {
      this.once('ready', () => this._run())
    }
  }
}
