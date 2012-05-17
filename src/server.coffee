express     = require 'express'
Router      = express.Router 
router      = new Router()
staticCache = express.staticCache()
static_     = express.static __dirname + '/server', {redirect: false}

plural_commands = 
  get: ['statistics']

singular_commands = 
  get: ['statistics', 'recently_finished_tasks', 'failed_tasks', 'slowest_tasks', 'processing_tasks', 'workers']
  post:['reschedule', 'clear']

exports = module.exports = (options) ->
  fairy = require('../.').connect options

  for method, commands of plural_commands
    for command_ in commands
      do (command_) ->
        router.route method, "/api/queues/#{command_}", (req, res) ->
          fairy[command_] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  for method, commands of singular_commands
    for command_ in commands
      do (command_) ->
        router.route method, "/api/queues/:name/#{command_}", (req, res) ->
          queue = fairy.queue req.params.name
          queue[command_] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  (req, res, next) ->
    router.middleware req, res, ->
      staticCache req, res, ->
        static_ req, res, next
