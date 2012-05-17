{exec} = require 'child_process'
fs = require 'fs'
require 'should'
task = 'TEST0'
fairy = require("#{__dirname}/..").connect()
queue = fairy.queue task
total = 2000
groups = 10
generated = 0
group_sequence = [0 .. groups - 1].map -> 0
child_processes = []

module.exports = 

  'should clear the queue first': (done) ->
    queue.clear (err, statistics) ->
      statistics.total.groups.should.equal 0
      statistics.total.tasks.should.equal 0
      done()

  'should successfully enqueued': (done) ->
    do generate = ->
      if generated++ is total
        queue.statistics (err, statistics) ->
          statistics.total.groups.should.equal groups
          statistics.total.tasks.should.equal total
          done()
      else
        group = parseInt Math.random() * groups
        sequence = group_sequence[group]++
        queue.enqueue group, sequence, generate

  'should all be processed': (done) ->
    exec "rm -f #{__dirname}/workers/*.dmp", (err, stdout, stderr) ->
      total_process = 8
      child_processes = while total_process--
        exec "coffee #{__dirname}/workers/perfect.coffee" # , (err, stdout, stderr) -> console.log err, stdout, stderr
      do probe = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total
            statistics.pending_tasks.should.equal 0
            statistics.processing_tasks.should.equal 0
            done()
          else
            setTimeout probe, 10

  'should cleanup elegantly on interruption': (done) ->
    child_processes.forEach (process) -> process.kill 'SIGINT'
    setTimeout ->
      queue.statistics (err, statistics) ->
        statistics.workers.should.equal 0
        done()
    , 100

  'should produce sequential results': (done) ->
    [0..groups-1].forEach (group) ->
      dump_file = fs.readFileSync("#{__dirname}/workers/#{group}.dmp").toString()
      dump_file.split('\n')[0..-2].forEach (content, line) ->
        content.should.equal line + ''
    exec "rm -f #{__dirname}/workers/*.dmp", -> done()
