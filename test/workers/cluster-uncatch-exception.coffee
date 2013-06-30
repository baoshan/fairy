cluster = require "cluster"
closing = off
if cluster.isMaster
  for i in [0...8]
    cluster.fork()
    # cluster.on 'exit', (worker) ->
  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...'
    cluster.fork() unless closing
  soft_kill_signals = [
    'SIGINT'
    'SIGHUP'
    'SIGQUIT'
    'SIGUSR1'
    'SIGUSR2'
    'SIGTERM'
    'SIGABRT'
  ]
  for signal in soft_kill_signals
    do (signal) ->
      process.on signal, ->
        console.log signal, ' in MASTER'
        closing = on
        # worker.send 'SIGINT' for id, worker of cluster.workers
        worker.process.kill(signal) for id, worker of cluster.workers
        # process.exit()
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
