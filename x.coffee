process.on 'exit', -> console.log 'a'
process.removeListener 'exit'
process.on 'exit', -> console.log 'b'

someSSS()
console.log 123
