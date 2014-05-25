fs     = require 'fs'
{exec} = require 'child_process'
should = require 'should'

Array::random = -> @[parseInt Math.random() * @length]

soft_kill_signals = [
  'SIGINT'
  'SIGHUP'
  'SIGQUIT'
  'SIGUSR1'
  'SIGUSR2'
  'SIGTERM'
  'SIGABRT'
]

soft_kill_signals = [
  'SIGTERM'
]

exports = module.exports =

  clear_queue: (queue, done) ->
    # setTimeout ->
    queue.clear (err, statistics) ->
      should.not.exist err
      # console.log queue.name, statistics
      statistics.total.groups.should.equal 0
      statistics.total.tasks.should.equal 0
      statistics.pending_tasks.should.equal 0
      done()
        # , 100

  enqueue_tasks: (queue, total_groups, total_tasks, done) ->
    generated = 0
    group_sequence = [0 .. total_groups - 1].map -> 0
    do generate = ->
      if generated++ is total_tasks
        return queue.statistics (err, statistics) ->
          should.not.exist err
          statistics.total.groups.should.equal total_groups
          statistics.total.tasks.should.equal total_tasks
          statistics.pending_tasks.should.equal total_tasks
          done()
      group = parseInt Math.random() * total_groups
      sequence = group_sequence[group]++
      queue.enqueue group, sequence, generate

  enqueue_tasks_wo_check: (queue, total_groups, total_tasks, done) ->
    generated = 0
    group_sequence = [0 .. total_groups - 1].map -> 0
    do generate = ->
      if generated++ is total_tasks
        return done()
      group = parseInt Math.random() * total_groups
      sequence = group_sequence[group]++
      queue.enqueue group, sequence, generate

  kill_one: (queue, done) ->
    queue.workers (err, workers) ->
      return done() unless workers.length
      try
        process.kill workers.random().pid, soft_kill_signals.random()
      done()

  wait_until_done: (queue, total_tasks, done) ->
    success_counter = 0
    do probe = ->
      # console.log 'probing'
      queue.statistics (err, statistics) ->
        # console.log 'statistics', err, statistics.finished_tasks
        if statistics.finished.tasks is total_tasks
          statistics.pending_tasks.should.equal 0
          statistics.processing_tasks.should.equal 0
          # console.log success_counter
          return done() if success_counter++ is 3
        setTimeout probe, 100

  clean_up: (queue, done) ->
    success_counter = 0
    setTimeout ->
      queue.workers (err, workers) ->
        for worker in workers
          process.kill worker.pid, soft_kill_signals.random()
        do get_statistics = ->
          queue.statistics (err, statistics) ->
            return setTimeout get_statistics, 100 unless statistics.workers is 0
            return setTimeout get_statistics, 100 unless success_counter++ is 3
            statistics.pending_tasks.should.equal 0
            done()
    , 2000

  clean_up_without_kill: (queue, done) ->
    success_counter = 0
    do get_statistics = ->
      queue.statistics (err, statistics) ->
        return setTimeout get_statistics, 100 unless statistics.workers is 0
        return setTimeout get_statistics, 100 unless success_counter++ is 3
        statistics.pending_tasks.should.equal 0
        done()

  check_result: (total_groups, done) ->
    for group in [0 .. total_groups - 1]
      for content, line in fs.readFileSync("#{__dirname}/workers/#{group}.dmp").toString().split('\n')[0...-1]
        content.should.equal "#{line}"
    exec "rm -f #{__dirname}/workers/*.dmp", done
