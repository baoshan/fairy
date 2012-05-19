express     = require 'express'
router      = new express.Router()
staticCache = express.staticCache()
static_     = express.static __dirname + '/server'

plural_commands = 
  get: ['statistics']

singular_commands = 
  get: ['statistics', 'recently_finished_tasks', 'failed_tasks', 'slowest_tasks', 'processing_tasks', 'workers']
  post:['reschedule', 'clear']

exports = module.exports = (options) ->
  fairy = require('../.').connect options

  for method, commands of plural_commands
    for command in commands
      router.route method, "/api/queues/#{command}", do (command) ->
        (req, res) ->
          fairy[command] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  for method, commands of singular_commands
    for command in commands
      router.route method, "/api/queues/:name/#{command}", do (command) ->
        (req, res) ->
          queue = fairy.queue req.params.name
          queue[command] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  (req, res, next) ->
    router.middleware req, res, ->
      staticCache req, res, ->
        static_ req, res, next
