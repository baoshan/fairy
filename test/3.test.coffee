{exec} = require 'child_process'
should = require 'should'
fairy  = require("..").connect()
{clear_queue, enqueue_tasks_wo_check, kill_one, wait_until_done, clean_up, check_result} = require './shared_steps'

task_name = 'TEST3'
queue     = fairy.queue task_name

total_groups = 10
total_tasks  = 2000
total_workers = require('os').cpus().length
child_processes = []

exiting = off
killed = 0
describe "Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} Fail-n-Block Workers, Kill and Respawn Periodically, Start Workers First", ->

  it "Should Clear the Queue First", (done) ->
    clear_queue queue, done

  it "Should start workers successfully", (done) ->
    while total_workers--
      do create_worker = ->
        exec("coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}").on 'exit', ->
          return if exiting
          killed++
          create_worker()

    do probe_workers = ->
      queue.statistics (err, statistics) ->
        if statistics.workers is require('os').cpus().length
          done()
        else
          setTimeout probe_workers, 1000

  it "Should Enqueue #{total_tasks} Tasks Successfully", (done) ->
    enqueue_tasks_wo_check queue, total_groups, total_tasks, done

  it "Should All Be Processed on a Interrupt and Respawn Environment", (done) ->

    # while total_workers-- > 0
    #  do create_worker = ->
    #    exec("coffee #{__dirname}/workers/fail-and-block.coffee #{task_name}").on 'exit', ->
    #      console.log 'on exit'
    #      return if exiting
    #      killed++
    #      create_worker()

    do reschedule = ->
      queue.reschedule (err, statistics) ->
        setTimeout reschedule, 100

    do kill_one_periodically = ->
      kill_one queue, ->
      setTimeout kill_one_periodically, 100

    wait_until_done queue, total_tasks, ->
      exiting = on
      done()

  it "Should Cleanup Elegantly on Interruption", (done) ->
    clean_up queue, done

  it "Should Dump Incremental Numbers", (done) ->
    check_result total_groups, done
