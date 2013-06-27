fairy = require("..").connect()
queue = fairy.queue process.argv[2]
queue.statistics (error, statistics) ->
  console.log statistics
  process.exit()
