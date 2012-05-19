express = require 'express'
app = express.createServer()
app.use new (require('../lib/server.js'))({host: '127.0.0.1'})
app.listen(3004)
console.log 'run servering'
