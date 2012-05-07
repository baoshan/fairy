require("#{__dirname}/..").connect().queue('task').reschedule ->
  console.log 'rescheduled successful'
  process.exit()
