fs     = require 'fs'
{exec} = require 'child_process'
should = require 'should'

exports = module.exports =

  clear_queue: (queue, done) ->
    queue.clear (err, statistics) ->
      should.not.exist err
      statistics.total.groups.should.equal  0
      statistics.total.tasks.should.equal   0
      statistics.pending_tasks.should.equal 0
      done()

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

  clean_up: (queue, done) ->
    checked_times = 0
    queue.workers (err, workers) ->
      allowed_signals = ['SIGINT', 'SIGHUP', 'SIGUSR1', 'SIGUSR2', 'SIGTERM', 'SIGQUIT', 'SIGTERM', 'SIGABRT']
      random_signal = -> allowed_signals[parseInt Math.random() * allowed_signals.length]
      process.kill worker.pid, random_signal() for worker in workers
      do get_statistics = ->
        queue.statistics (err, statistics) ->
          return setTimeout get_statistics, 100 unless statistics.workers is 0
          return setTimeout get_statistics, 100 unless checked_times++ is 3
          done()

  check_result: (total_groups, done) ->
    for group in [0 .. total_groups - 1]
      for content, line in fs.readFileSync("#{__dirname}/workers/#{group}.dmp").toString().split('\n')[0..-2]
        content.should.equal "#{line}"
    exec "rm -f #{__dirname}/workers/*.dmp", done
