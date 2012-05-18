express = require 'express'
server  = new (require('../lib/server.js'))({host: '127.0.0.1'})
app     = express.createServer()
app.use server
app.listen(3004)
console.log 'run servering'
