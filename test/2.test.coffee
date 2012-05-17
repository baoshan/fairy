
{exec} = require 'child_process'
fs     = require 'fs'
should = require 'should'
fairy  = require("..").connect()

task_name = 'TEST2'
queue     = fairy.queue task_name

total_groups = 10
total_tasks  = 2000
total_workers = require('os').cpus().length
child_processes = []

describe "Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Fail-n-Block Workers, Kill and Respawn Periodically", ->

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

  it "Should All Be Processed on a Interrupt and Respawn Environment", (done) ->
    exiting = off
    exec "rm -f #{__dirname}/workers/*.dmp", (err, stdout, stderr) ->
      workers_left = total_workers
      child_processes = []
      while --workers_left >= 0
        do (workers_left) ->
          child_processes[workers_left] = exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"
          child_processes[workers_left].on 'exit', ->
            return if exiting
            child_processes[workers_left] = exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"

      do reschedule = ->
        queue.reschedule (err, statistics) ->
        setTimeout reschedule, 100

      do killone = ->
        victim_index = parseInt Math.random() * (child_processes.length - 1)
        child_processes[victim_index].kill 'SIGHUP'
        setTimeout killone, 100

      do stats = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total_tasks
            setTimeout ->
              queue.statistics (err, statistics) ->
                if statistics.finished_tasks is total_tasks and statistics.pending_tasks is 0
                  exiting = on
                  # child_processes.forEach (process) -> process.kill 'SIGHUP'
                  done()
            , 100
          else
            setTimeout stats, 10

  it "Should Cleanup Elegantly on Interruption", (done) ->
    allowed_signals = ['SIGINT', 'SIGHUP', 'SIGUSR2']
    random_signal = -> allowed_signals[parseInt Math.random() * allowed_signals.length]
    process.kill random_signal() for process in child_processes
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
