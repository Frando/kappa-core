const hypercoreSource = require('./hypercore')
const { mergePull, mergeReset } = require('./util/merge')
const SimpleState = require('./util/state')

module.exports = function multifeedSource (opts) {
  const state = new SimpleState(opts)
  const feeds = opts.feeds
  const scopeFeeds = opts.scopeFeeds || function (feed, cb) { cb() }
  const sources = []

  return {
    open (flow, next) {
      feeds.ready(() => {
        feeds.feeds().forEach(feed => onfeed(flow, feed))
        feeds.on('feed', feed => onfeed(flow, feed))
        next()
      })
    },
    pull (next) {
      mergePull(sources, next)
    },
    reset (next) {
      mergeReset(sources, next)
    },
    fetchVersion: state.fetchVersion,
    storeVersion: state.storeVersion,
    api: {
      feed (kappa, key) {
        return feeds.feed(key)
      }
    }
  }

  function onfeed (flow, feed, cb) {
    scopeFeeds(feed, (err) => {
      if (err && cb) return cb()
      if (err) return

      const source = hypercoreSource({
        feed,
        state
      })
      sources.push(source)
      source.open(flow, () => {
        flow.update()
        if (cb) cb()
      })
    })
  }
}
