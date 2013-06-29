cluster = require "cluster"

if cluster.isMaster
  for i in [0...8]
    cluster.fork()
  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...'
    cluster.fork()
else
  console.log 'slave process'
  {exec} = require 'child_process'
  task      = process.argv[2]
  fairy     = require("#{__dirname}/../..").connect()
  queue     = fairy.queue task

  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          callback null
    , Math.random() * 1
