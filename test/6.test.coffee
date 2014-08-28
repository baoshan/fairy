{exec} = require 'child_process'
should = require 'should'
fairy  = require("..").connect()
{clear_queue, enqueue_tasks, kill_one, wait_until_done, clean_up_without_kill, check_result} = require './shared_steps'

task_name = 'TEST6'
queue     = fairy.queue task_name

total_groups = 5
total_tasks  = 200
total_workers = require('os').cpus().length
child_processes = []

describe "in cluster, Process #{total_tasks} Tasks of #{total_groups} Groups by #{total_workers} uncatch-exception Workers, Kill and run normally", ->

  @timeout(200000)

  it "Should Clear the Queue First", (done) ->
    clear_queue queue, done

  it "Should Enqueue #{total_tasks} Tasks Successfully", (done) ->
    enqueue_tasks queue, total_groups, total_tasks, done

  it "Should All Be Processed on a Interrupt and Respawn Environment", (done) ->
    exiting = off
    killed = 0

    while total_workers-- > 0
      do create_worker = ->
        child_processes.push exec("coffee #{__dirname}/workers/cluster-uncatch-exception-master-worker.coffee #{task_name}", (a, b, c) ->
          return
          console.log a, b, c).on 'exit', ->
            return if exiting
            # console.log 're creating'
            killed++
            create_worker()

    do reschedule = ->
      queue.retry (err, statistics) ->
        setTimeout reschedule, 100

    # console.log 'wait_until_done'
    wait_until_done queue, total_tasks, ->
      exiting = on
      done()

  it "Should Cleanup Elegantly on Interruption", (done) ->
    for child_process in child_processes
      # console.log 'killing', child_process.pid
      exec("/bin/bash -c 'kill -SIGINT #{child_process.pid}'", (err, res) -> ) # console.log err, res)
      # child_process.kill()
    setTimeout ->
      clean_up_without_kill queue, done
    , 100

  return
  it "Should Dump Incremental Numbers", (done) ->
    check_result total_groups, done
