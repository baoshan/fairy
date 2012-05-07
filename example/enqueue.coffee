# Enqueue Jobs

task      = 'task'
yamg      = require("#{__dirname}/..").connect()
queue     = yamg.queue task
total     = process.argv[2] or 10000
groups    = 10
generated = 0

group_sequence = [0 .. groups - 1].map -> 0

start = new Date

do generate = ->
  if generated++ >= total
    elapsed = new Date - start
    console.log 'finished in', elapsed, 'milliseconds'
    console.log 'speed: ', total / elapsed * 1000, 'jobs / s'
    process.exit()
  else
    group = parseInt Math.random() * groups
    sequence = group_sequence[group]++
    queue.enqueue group, sequence, generate
