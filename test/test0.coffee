
{exec} = require 'child_process'
fs = require 'fs'
require 'should'
task = 'TEST0'
fairy = require("#{__dirname}/..").connect()
queue = fairy.queue task
total = 10000
groups = 10
generated = 0
group_sequence = [0 .. groups - 1].map -> 0
child_processes = []

describe "Basic test enqueues #{total} tasks, which", ->

  it 'should clear the queue first', (done) ->
    queue.clear (err, statistics) ->
      statistics.total.groups.should.equal 0
      statistics.total.tasks.should.equal 0
      done()

  it 'should successfully enqueued', (done) ->
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

  it 'should all be processed', (done) ->
    exec "rm -f #{__dirname}/test0/*.dmp", (err, stdout, stderr) ->
      total_process = 50
      child_processes = while total_process--
        exec "coffee #{__dirname}/test0/0_process.coffee"
      do probe = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total
            statistics.pending_tasks.should.equal 0
            statistics.processing_tasks.should.equal 0
            done()
          else
            setTimeout probe, 10

  it 'should cleanup elegantly on interruption', (done) ->
    child_processes.forEach (process) -> process.kill 'SIGINT'
    setTimeout ->
      queue.statistics (err, statistics) ->
        statistics.workers.should.equal 0
        done()
    , 100

  it 'should produce sequential results', (done) ->
    [0..groups-1].forEach (group) ->
      dump_file = fs.readFileSync("#{__dirname}/test0/#{group}.dmp").toString()
      dump_file.split('\n')[0..-2].forEach (content, line) ->
        content.should.equal line + ''
    exec "rm -f #{__dirname}/test0/*.dmp", -> done()
