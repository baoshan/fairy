fairy = require('..').connect()
queue = fairy.queue('TEST7')
uuid  = require('node-uuid')
should = require 'should'

describe "Enqueuer should receive progress notification", ->

  @timeout(200000)

  it "Should Clear the Queue First", ->
    queue.regist (group, nonsense, callback, progress) ->
      progress('progress')
      progress('progress')
      callback(null, 'result')

  it 'Should Enqueue Successfully', (done) ->
    progressed = 0
    enqueued = off
    queue.enqueue uuid.v4(), 'nonsense', (->
      enqueued.should.equal(off)
      enqueued = on
    ), ((result) ->
      result.should.equal('result')
      progressed.should.equal(2)
      done()
    ), (progress) ->
      progressed++
      progress.should.equal('progress')
