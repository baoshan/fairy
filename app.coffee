connect = require 'connect'
server  = new (require('./src/server.coffee'))({host: '127.0.0.1'})
app     = connect.createServer()
app.use server
app.listen(3004)
console.log 'run servering'
