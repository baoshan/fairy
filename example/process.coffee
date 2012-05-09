# Processing Job from Queue.

{exec} = require 'child_process'
task   = 'task'
yamg   = require("#{__dirname}/..").connect()
queue  = yamg.queue task
queue.regist (group, sequence, callback) ->
  console.log 'processing', group, sequence
  doNotExistsFunction() if Math.random() < 0.001
  exec 'echo ' + sequence + ' >> ' + group + '.dmp', ->
    if Math.random() < 0.1
      callback {do: if Math.random() < 0.5 then 'skip-after-retry' else 'block-after-retry'}, null
    else
      callback null, 'OK'
