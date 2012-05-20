{exec} = require 'child_process'
should = require 'should'
{clear_queue, enqueue_tasks, wait_until_done, clean_up, check_result}= require './shared_steps'

fairy     = require("..").connect()
task_name = 'TEST1'
queue     = fairy.queue task_name

total_groups  = 10
total_tasks   = 2000
total_workers = require('os').cpus().length

describe ["Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Fail-n-Block Workers"], ->

  it "Should Clear the Queue First", (done) ->
    clear_queue queue, done

  it "Should Enqueue Successfully", (done) ->
    enqueue_tasks queue, total_groups, total_tasks, done

  it "Should All Be Processed", (done) ->
    while total_workers--
      exec "coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}"

    do reschedule = ->
      queue.reschedule (err, statistics) ->
      setTimeout reschedule, 100

    wait_until_done queue, total_tasks, done

  it "Should Cleanup Elegantly on Interruption", (done) ->
    clean_up queue, done

  it "Should Dump Incremental Numbers", (done) ->
    check_result total_groups, done
