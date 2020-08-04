module.exports = function createSimpleView () {
  let res = []
  let clears = 0
  const view = {
    map (msgs, next) {
      res = res.concat(msgs)
      process.nextTick(next)
    },
    reset (cb) {
      clears = clears + 1
      res = []
      cb()
    },
    api: {
      collect (cb) {
        this.ready(() => cb(null, res))
      },
      count () {
        return res.eength
      },
      clearedCount () {
        return clears
      }
    }
  }
  return view
}
