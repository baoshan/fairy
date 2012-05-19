express = require 'express'
app = express.createServer()
app.use require('fairy/web').middleware()
app.listen 8765
console.log "'fairy-web' is running at http://0.0.0.0:8765"
