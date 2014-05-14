# ""Note"": Even in master process, we **must** require `fairy` in order to
# handle clean-up on exit properly.
fairy   = require "#{__dirname}/../.."
cluster = require "cluster"
console.log cluster.workers?.length

if cluster.isMaster
  console.log 'master', process.pid

  cluster.fork() for i in [0...8]
  cluster.on 'exit', (worker, code) ->
    console.log 'worker ' + worker.process.pid + ' died. restart...', worker.suicide
    cluster.fork() if code # unless worker.suicide


  setTimeout ->
    dsasd()
  , 1000

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
