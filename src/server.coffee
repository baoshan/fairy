express     = require 'express'
Router      = express.Router 
router      = new Router()
staticCache = express.staticCache()
stc         = express.static(__dirname + '/server', {redirect: false})

plural_commands = 
  get: ['statistics']

single_commands = 
  post: ['reschedule','clear']
  get: ['statistics','recently_finished_tasks','failed_tasks','slowest_tasks', 'processing_tasks','workers']

exports = module.exports = (options) ->
  fairy = require('../.').connect options
  for key,value of plural_commands
    for element in value
      router.route key, '/api/queues/'+ element, (req, res) ->
        fairy.statistics (err, results) ->
          return res.send 500, err.stack if err
          res.send results
  for key,value of single_commands
    for element in value
      router.route key, '/api/queues/:name/'+ element, (req, res) ->
        queue = fairy.queue req.params.name
        queue[element] (err, results) ->
          return res.send 500, err.stack if err
          res.send results
  (req, res, next) ->
    router.middleware req, res, ->
      staticCache req, res, ->
        stc(req, res, next)
