connect = require 'connect'

exports = module.exports = (options) ->
  fairy = require('../fairy.coffee').connect options
  connect.static(__dirname + '/server',{ maxAge: 86400000 })
  (req, res, next) ->
    switch req.url
      when '/api/queues/statistics' 
        fairy.statistics (stats) ->
          res.send stats
      when '/api/queues/:name/reschedule'
        queue = fairy.queue req.params.name
        queue.reschedule (stats) ->
          res.send stats
      when '/api/queues/:name/clear'
        queue = fairy.queue req.params.name
        queue.clear (stats) ->
          res.send stats
      when '/api/queues/:name/statistics'
        queue = fairy.queue req.params.name
        queue.statistics (stats) ->
          res.send stats
      when '/api/queues/:name/recently_finished_tasks'
        queue = fairy.queue req.params.name
        queue.recently_finished_tasks (tasks) ->
          res.send tasks
      when '/api/queues/:name/failed_tasks'
        queue = fairy.queue req.params.name
        queue.failed_tasks (tasks) ->
          res.send tasks
      when '/api/queues/:name/slowest_tasks'
        queue = fairy.queue req.params.name
        queue.slowest_tasks (tasks) ->
          res.send tasks
      when '/api/queues/:name/processing_tasks'
        queue = fairy.queue req.params.name
        queue.processing_tasks (tasks) ->
          res.send tasks
      when '/api/queues/:name/workers'
        queue = fairy.queue req.params.name
        queue.workers (workers) ->
          res.send workers
    next()
