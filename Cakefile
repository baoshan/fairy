{spawn, exec} = require 'child_process'

task 'build', 'Build the fairy source code', ->
  coffee = spawn 'coffee', ['-o', 'lib', 'src']
  coffee.stdout.on 'data', (data) -> console.log data.toString()
  coffee.stderr.on 'data', (data) -> console.error data.toString()
  
