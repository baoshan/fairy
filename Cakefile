{spawn, exec} = require 'child_process'

option '-w', '--watch', 'continually build the fairy library'

task 'build', 'Build the fairy source code', (options) ->
  exec([
    'mkdir -p lib/server'
    'cp src/server/fairy.html lib/server/fairy.html'
    'cp src/server/fairy.css  lib/server/fairy.css'
    'cp src/server/fairy_active.js lib/server/fairy_active.js'
  ].join(' && '), (err, stdout, stderr) ->
    console.error stderr if err
  )
  coffee = spawn 'coffee', ['-c' + (if options.watch then 'w' else ''), '-o', 'lib', 'src']
  coffee.stdout.on 'data', (data) -> console.log data.toString()
  coffee.stderr.on 'data', (data) -> console.error data.toString()
