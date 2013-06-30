cluster = require "cluster"

if cluster.isMaster
  for i in [0...8]
    cluster.fork()
    # cluster.on 'exit', (worker) ->
    # cluster.on 'exit', (worker, code, signal) -> console.log code, signal
    # console.log 'worker ' + worker.process.pid + ' died. restart...'
    # child_processes.push cluster.fork()
  process.on 'SIGTERM', ->
    # worker.send 'SIGINT' for id, worker of cluster.workers
    worker.process.kill() for id, worker of cluster.workers
else
  console.log 'slave process'
  # process.on 'SIGTERM', -> console.log 'SIGTERM'
  {exec} = require 'child_process'
  task      = process.argv[2]
  fairy     = require("#{__dirname}/../..").connect()
  queue     = fairy.queue task
  #setTimeout (->), 100000
  #return
  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          callback null
    , Math.random() * 1
