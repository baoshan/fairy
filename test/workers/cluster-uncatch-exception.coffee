cluster = require "cluster"
fairy = require "#{__dirname}/../.."

if cluster.isMaster

  exiting = off
  cluster.fork() for i in [0...8]

  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...', worker.suicide
    cluster.fork() unless worker.suicide

  return

  soft_kill_signals = [
    'SIGINT'
    'SIGHUP'
    'SIGQUIT'
    'SIGUSR1'
    'SIGUSR2'
    'SIGTERM'
    'SIGABRT'
  ]

  for soft_kill_signal in soft_kill_signals
    do (soft_kill_signal) ->
      process.on soft_kill_signal, ->
        exiting = on
        worker.process.kill(soft_kill_signal) for id, worker of cluster.workers

else

  {exec} = require 'child_process'
  task      = process.argv[2]
  fairy     = fairy.connect()
  queue     = fairy.queue task

  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          callback null
    , Math.random() * 1
