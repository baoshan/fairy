express   = require 'express'
fairy_web = require 'fairy/web'

app = express.createServer()
app.use fairy_web.middleware
app.listen 8765

console.log "'fairy-web' is running at http://0.0.0.0:8765"
