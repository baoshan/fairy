{spawn, exec} = require 'child_process'

option '-w', '--watch', 'continually build the fairy library'

task 'build', 'Build the fairy source code', (options) ->
  coffee = spawn 'coffee', ['-c' + (if options.watch then 'w' else ''), '-o', 'lib', 'src']
  coffee.stdout.on 'data', (data) -> console.log data.toString()
  coffee.stderr.on 'data', (data) -> console.error data.toString()
  
