express = require 'express'
fairy_middleware = new (require('../lib/server.js'))({host: '127.0.0.1'})
app     = express.createServer()
app.use fairy_middleware
app.listen(3004)
console.log 'run servering'
