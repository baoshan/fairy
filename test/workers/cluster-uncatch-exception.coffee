fairy   = require "#{__dirname}/../.."
cluster = require "cluster"

if cluster.isMaster

  exiting = off
  cluster.fork() for i in [0...8]

  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...', worker.suicide
    cluster.fork() unless worker.suicide

else

  {exec} = require 'child_process'
  task   = process.argv[2]
  fairy  = fairy.connect()
  queue  = fairy.queue task

  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          callback null
    , Math.random() * 1
