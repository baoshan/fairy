fairy = require ('..')
fairy = fairy.connect()
queue = fairy.queue ('q')

queue.regist (group, callback) ->
  callback()


setTimeout ->
  abc()
, 10000
