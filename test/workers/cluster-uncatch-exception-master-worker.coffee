# ""Note"": Even in master process, we **must** require `fairy` in order to
# handle clean-up on exit properly.
fairy   = require "#{__dirname}/../.."
cluster = require "cluster"
task   = process.argv[2]
{exec} = require 'child_process'

if cluster.isMaster

  cluster.fork() for i in [0...8]
  cluster.on 'exit', (worker) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...', worker.suicide
    cluster.fork() unless worker.suicide

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

  console.log 'REG'
  fairy  = require "#{__dirname}/../.."
  fairy  = fairy.connect()
  console.log task
  queue  = fairy.queue task

  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          console.log 'success'
          callback null
    , Math.random() * 1

else

  console.log 'SLAVE'
  fairy  = fairy.connect()
  queue  = fairy.queue task

  queue.regist (group, sequence, callback) ->
    setTimeout ->
      if Math.random() < 0.1
        abc()
      else
        exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
          console.log 'success'
          callback null
    , Math.random() * 1
