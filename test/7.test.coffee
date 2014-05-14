fairy = require('..').connect()
queue = fairy.queue('TEST7')
uuid  = require('node-uuid')
should = require 'should'

describe ["Enqueuer should receive progress notification"], ->

  it "Should Clear the Queue First", ->
    queue.regist (group, nonsense, callback, progress) ->
      progress('progress')
      callback(null, 'result')

  it 'Should Enqueue Successfully', (done) ->
    progressed = off
    enqueued = off
    queue.enqueue uuid.v4(), 'nonsense', (->
      enqueued.should.equal(off)
      enqueued = on
    ), ((result) ->
      result.should.equal('result')
      progressed.should.equal(on)
      done()
    ), (progress) ->
      enqueued.should.equal(on)
      progress.should.equal('progress')
      progressed.should.equal(off)
      progressed = on
