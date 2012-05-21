require("#{__dirname}/..").connect().queue('task').reschedule (err, statistics) ->
  return console.log 'reschedule failed:', err if err
  console.log 'reschedule successed:', statistics
  process.exit()
