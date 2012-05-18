{exec} = require 'child_process'

task      = process.argv[2]
fairy     = require("#{__dirname}/../..").connect()
queue     = fairy.queue task

queue.regist (group, sequence, callback) ->
  setTimeout ->
    if Math.random() < 0.1
      callback {do: if Math.random() < 0.5 then 'block' else 'block-after-retry'}
    else
      exec 'echo ' + sequence + ' >> ' + "#{__dirname}/" + group + '.dmp', ->
        callback null
  , Math.random() * 1
