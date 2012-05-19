express = require 'express'
app = express.createServer()
app.use require('fairy/server').middleware()
app.listen(3004)
console.log 'run servering'
