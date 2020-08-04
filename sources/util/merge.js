exports.mergePull = function (sources, next) {
  if (!sources.length) return process.nextTick(next)
  let results = []
  let idx = -1
  onnext()
  function onnext () {
    idx++
    if (!sources[idx]) return onfinish()
    sources[idx].pull(onresult)
  }
  function onresult (error, messages, finished, onindexed) {
    results.push({ error, messages, finished, onindexed })
    process.nextTick(onnext)
  }
  function onfinish () {
    if (!results.length) return next()
    const messages = []
    let finished = true
    for (const result of results) {
      if (result.messages) Array.prototype.push.apply(messages, result.messages)
      if (!result.finished) finished = false
    }
    const error = mergeErrors(results.map(r => r.error))
    next(error, messages, finished, onindexed)
  }
  function onindexed (cb) {
    let fns = results.map(r => r.onindexed).filter(f => f)
    if (!fns.length) return cb()
    let pending = fns.length
    fns.forEach(fn => fn(done))
    function done () {
      if (--pending === 0) cb()
    }
  }
}

exports.mergeReset = function (sources, cb) {
  let pending = sources.length
  sources.forEach(source => source.reset(done))
  function done () {
    if (--pending === 0) cb()
  }
}

function mergeErrors (errs) {
  if (!errs) return null
  errs = errs.filter(e => e)
  if (!errs.length) return null
  if (errs.length === 1) return errs[0]
  const err = new Error(`${errs.length} errors closing views`)
  err.errors = errs
  return err
}
