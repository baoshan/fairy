express     = require 'express'
router      = new express.Router()
fs          = require "fs"
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
      router.route method, "/api/queues/#{command}", no_cache, do (command) ->
        (req, res) ->
          fairy[command] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  for method, commands of singular_commands
    for command in commands
<<<<<<< HEAD
      router.route method, "/api/queues/:name/#{command}", no_cache, do (command) ->
=======
      router.route method, "/api/queues/:name/#{command}", do (command) ->
>>>>>>> f834d4e6b52186da911b01aa9dddd192133daef5
        (req, res) ->
          queue = fairy.queue req.params.name
          queue[command] (err, results) ->
            return res.send 500, err.stack if err
            res.send results

  (req, res, next) ->
    router.middleware req, res, ->
      req.url = '/fairy.html' if req.url is '/fairy'
      staticCache req, res, ->
        static_ req, res, next

no_cache = (req, res, next) ->
  res.setHeader "Cache-Control", "no-cache"
  next()
