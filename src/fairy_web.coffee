express = require 'express'
fairy   = require('./fairy.coffee').connect()
app  = express.createServer()

app.set 'view engine', 'jade'
app.use express.bodyParser()
app.use app.router
app.use express.static __dirname + '/static', {redirect: false}
app.use express.compiler 
  src: __dirname+'/static'
  dest: __dirname+'/static'
  enable: ['coffeescript']

app.get '/queue', (req, res) -> res.render 'queue', { layout: false }

app.get '/api/queues/statistics', (req, res) -> 
  fairy.statistics (stats) ->
    res.send stats

app.get '/api/queues/:name/statistics', (req, res) ->
  queue = fairy.queue req.params.name
  queue.statistics (stats) ->
    res.send stats

app.get '/api/queues/:name/recently_finished_tasks', (req, res) ->
  queue = fairy.queue req.params.name
  queue.recently_finished_tasks (tasks) ->
    res.send tasks

app.get '/api/queues/:name/failed_tasks', (req, res) ->
  queue = fairy.queue req.params.name
  queue.failed_tasks (tasks) ->
    res.send tasks

app.get '/api/queues/:name/slowest_tasks', (req, res) ->
  queue = fairy.queue req.params.name
  queue.slowest_tasks (tasks) ->
    res.send tasks

app.post '/api/queues/:name/reschedule', (req, res) ->
  queue = fairy.queue req.params.name
  queue.reschedule (stats) -> 
    res.send stats

app.get '/api/queues/:name/workers', (req, res) ->
  queue = fairy.queue req.params.name
  queue.workers (workers) ->
    res.send workers

app.listen 3004
console.log 'server running'
