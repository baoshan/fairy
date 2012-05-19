express = require 'express'
app = express.createServer()
app.use require('fairy/web').middleware()
app.listen(3004)
console.log 'run servering'
