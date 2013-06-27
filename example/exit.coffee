cluster = require "cluster"

if cluster.isMaster
  for i in [0...8]
    cluster.fork()
  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...'
    cluster.fork()

else
  console.log 'slave process'
  fairy = require("..").connect()
  queue = fairy.queue "TEST3"
  queue.regist (group, callback) ->
    setTimeout callback, 1000
  seconds = Math.random() * 1000 * 20
  setTimeout (-> abc()), seconds



