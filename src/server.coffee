connect = require 'connect'
Router  = require('express').Router 
router = new Router()

exports = module.exports = (options) ->
  fairy = require('./fairy.coffee').connect options
  router.route 'get', '/api/queues/statistics',(req, res, next) ->
    fairy.statistics (err, stats) ->
      return reswrite(res, err.stack) if err
      reswrite(res, stats)
  router.route 'post', '/api/queues/:name/reschedule',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.reschedule (err, stats) ->
      return reswrite(res, err.stack) if err
      reswrite(res, stats)
  router.route 'post', '/api/queues/:name/clear',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.clear (err, stats) ->
      return reswrite(res, err.stack) if err
      reswrite(res, stats)
  router.route 'get', '/api/queues/:name/statistics',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.statistics (err, stats) ->
      return reswrite(res, err.stack) if err
      reswrite(res, stats)
  router.route 'get', '/api/queues/:name/recently_finished_tasks',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.recently_finished_tasks (err, tasks) ->
      return reswrite(res, err.stack) if err
      reswrite(res, tasks)
  router.route 'get', '/api/queues/:name/failed_tasks',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.failed_tasks (err, tasks) ->
      return reswrite(res, err.stack) if err
      reswrite(res, tasks)
  router.route 'get', '/api/queues/:name/slowest_tasks',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.slowest_tasks (err, tasks) ->
      return reswrite(res, err.stack) if err
      reswrite(res, tasks)
  router.route 'get', '/api/queues/:name/processing_tasks',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.processing_tasks (err, tasks) ->
      return reswrite(res, err.stack) if err
      reswrite(res, tasks)
  router.route 'get', '/api/queues/:name/workers',(req, res, next) ->
    queue = fairy.queue req.params.name
    queue.workers (err, workers) ->
      return reswrite(res, err.stack) if err
      reswrite(res, workers)
  (req, res, next) ->
    router.middleware req, res, -> 
      connect.static(__dirname + '/server',{ maxAge: 86400000 })(req, res, next)

reswrite = (res, content) ->
  res.writeHead 200, { 'Content-Type': 'application/json'}
  res.write(JSON.stringify(content))
  res.end()
