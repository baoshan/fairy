{exec} = require 'child_process'
should = require 'should'
fairy  = require("..").connect()
{clear_queue, enqueue_tasks, clean_up, check_result} = require './shared_steps'

task_name = 'TEST2'
queue     = fairy.queue task_name

total_groups = 10
total_tasks  = 2000
total_workers = require('os').cpus().length
child_processes = []

describe "Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Fail-n-Block Workers, Kill and Respawn Periodically", ->

  it "Should Clear the Queue First", (done) ->
    clear_queue queue, done

  it "Should Enqueue #{total_tasks} Tasks Successfully", (done) ->
    enqueue_tasks queue, total_groups, total_tasks, done

  it "Should All Be Processed on a Interrupt and Respawn Environment", (done) ->
    exiting = off
    exec "rm -f #{__dirname}/workers/*.dmp", (err, stdout, stderr) ->
      killed = 0
      workers_left = total_workers
      child_processes = []
      while --workers_left >= 0
        do (workers_left) ->
          child_processes[workers_left] = exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"
          respawn = (workers_left) ->
            child_processes[workers_left] = exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"
            child_processes[workers_left].on 'exit', do (workers_left) ->
              ->
                killed++
                return if exiting
                respawn(workers_left)

          child_processes[workers_left].on 'exit', do (workers_left) ->
            ->
              killed++
              return if exiting
              respawn(workers_left)

      do reschedule = ->
        queue.reschedule (err, statistics) ->
        setTimeout reschedule, 100

      do killone = ->
        queue.workers (err, workers) ->
                  
          return setTimeout killone, 100 unless workers.length
          victim_index = parseInt Math.random() * workers.length
          allowed_signals = ['SIGINT', 'SIGHUP', 'SIGUSR2']
          random_signal = -> allowed_signals[parseInt Math.random() * allowed_signals.length]
          process.kill workers[victim_index].pid, random_signal()
          setTimeout killone, 100

      do stats = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total_tasks
            setTimeout ->
              queue.statistics (err, statistics) ->
                if statistics.finished_tasks is total_tasks and statistics.pending_tasks is 0
                  exiting = on
                  console.log ", #{killed} workers killed, #{statistics.workers} alive"
                  done()
            , 100
          else
            setTimeout stats, 10

  it "Should Cleanup Elegantly on Interruption", (done) ->
    clean_up queue, done

  it "Should Dump Incremental Numbers", (done) ->
    check_result total_groups, done
