{exec} = require 'child_process'
should = require 'should'
fairy  = require("..").connect()
{clear_queue, enqueue_tasks, clean_up, check_result}= require './shared_steps'
task_name = 'TEST1'
queue     = fairy.queue task_name

total_groups = 10
total_tasks  = 2000
total_workers = require('os').cpus().length
child_processes = []

describe ["Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Fail-n-Block Workers"], ->

  it "Should Clear the Queue First", (done) ->
    clear_queue queue, done

  it "Should Enqueue #{total_tasks} Tasks Successfully", (done) ->
    enqueue_tasks queue, total_groups, total_tasks, done

  it "Should All Be Processed", (done) ->
    exec "rm -f #{__dirname}/workers/*.dmp", (err, stdout, stderr) ->
      workers_left = total_workers
      child_processes = while workers_left--
        exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"
      do reschedule = ->
        queue.reschedule (err, statistics) ->
        setTimeout reschedule, 1000
      do probe = ->
        queue.statistics (err, statistics) ->
          if statistics.finished_tasks is total_tasks
            statistics.pending_tasks.should.equal 0
            statistics.processing_tasks.should.equal 0
            done()
          else
            setTimeout probe, 10

  it "Should Cleanup Elegantly on Interruption", (done) ->
    clean_up queue, done

  it "Should Dump Incremental Numbers", (done) ->
    check_result total_groups, done
