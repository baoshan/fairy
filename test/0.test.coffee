
{exec} = require 'child_process'
fs     = require 'fs'
should = require 'should'
fairy  = require("..").connect()

task_name = 'TEST0'
queue     = fairy.queue task_name

total_groups = 10
total_tasks  = 2000
total_workers = require('os').cpus().length
child_processes = []

describe ["Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Perfect Workers"], ->

  it "Should Clear the Queue First", (done) ->
    queue.clear (err, statistics) ->
      statistics.total.groups.should.equal 0
      statistics.total.tasks.should.equal 0
      done()

  it "Should Enqueue #{total_tasks} Tasks Successfully", (done) ->
    generated = 0
    group_sequence = [0 .. total_groups - 1].map -> 0
    do generate = ->
      if generated++ is total_tasks
        return queue.statistics (err, statistics) ->
          statistics.total.groups.should.equal total_groups
          statistics.total.tasks.should.equal total_tasks
          done()
      group = parseInt Math.random() * total_groups
      sequence = group_sequence[group]++
      queue.enqueue group, sequence, generate

  it "Should All Be Processed", (done) ->
    exec "rm -f #{__dirname}/workers/*.dmp", (err, stdout, stderr) ->
      workers_left = total_workers
      child_processes = while workers_left--
        exec "coffee #{__dirname}/workers/perfect.coffee #{task_name}"
      do probe = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total_tasks
            statistics.pending_tasks.should.equal 0
            statistics.processing_tasks.should.equal 0
            done()
          else
            setTimeout probe, 10

  it "Should Cleanup Elegantly on Interruption", (done) ->
    queue.workers (err, workers) ->
      allowed_signals = ['SIGINT', 'SIGHUP', 'SIGUSR2']
      random_signal = -> allowed_signals[parseInt Math.random() * allowed_signals.length]
      process.kill worker.pid, random_signal() for worker in workers
      do get_statistics = ->
        queue.statistics (err, statistics) ->
          return get_statistics() unless statistics.workers is 0
          setTimeout ->
            queue.statistics (err, statistics) ->
              done() if statistics.workers is 0
          , 10

  it "Should Dump Incremental Numbers", (done) ->
    for group in [0 .. total_groups - 1]
      for content, line in fs.readFileSync("#{__dirname}/workers/#{group}.dmp").toString().split('\n')[0..-2]
        content.should.equal "#{line}"
    exec "rm -f #{__dirname}/workers/*.dmp", done
