express     = require 'express'
router      = new express.Router()
static_     = express.static __dirname + '/web'

plural_commands =
  get: ['statistics']

singular_commands =
  get: ['statistics', 'recently_finished_tasks', 'failed_tasks', 'slowest_tasks', 'processing_tasks', 'workers']
  post:['reschedule', 'clear']

exports.connect = (options = {}) ->
  new Connect(options)

class Connect
  constructor: (@options) ->
    @fairy = require('../.').connect(options)
    @no_cache = (req, res, next) ->
      res.setHeader "Cache-Control", "no-cache"
      next()
    @__defineGetter__ 'middleware', =>
      for method, commands of plural_commands
        for command in commands
          router[method] "/api/queues/#{command}", @no_cache, do (command) =>
            (req, res) =>
              @fairy[command] (err, results) ->
                return res.send 500, err.stack if err
                res.send results

      for method, commands of singular_commands
        for command in commands
          router[method] "/api/queues/:name/#{command}", @no_cache, do (command) =>
            (req, res) =>
              queue = @fairy.queue req.params.name
              queue[command] (err, results) ->
                return res.send 500, err.stack if err
                res.send results
      
      router.use (req, res, next) ->
        req.url = '/fairy.html' if req.url is '/fairy'
        static_ req, res, next

