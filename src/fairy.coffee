# *Queue System Treats Tasks Fairly.*
#
# **Fairy** is a lightweight queue engine for node.js based on Redis. Fairy
# offers ActiveMQ's **[message groups]** alike feature which can guarantee
# the sequential processing order of tasks belong to a same group.
#
# [Message Groups]: http://activemq.apache.org/message-groups.html
#
# But, unkile **message groups**, **Fairy** doesn't always route tasks of a
# group to a same worker, which can lead to unwanted waiting time when:
#
#   1. Tasks of group `X` and `Y` are appointed to worker `A`.
#   2. Worker `A` is processing tasks of group `X` **sequentially**.
#   3. Tasks of group `Y` are pending, while:
#   4. Worker `B` is snoozing (because of 1)!
#
# **Fairy** will route the task of group `Y` to worker `B` in this scenario.
#
# **Fairy** takes a different approach than Message Groups. Instead of making
# all tasks of a same group be routed to the same consumer, **Fairy** route a
# task to any worker when there's no **processing** tasks of the same group.
#
# The design philosophy makes **Fairy** ideal for the following requirements:
#
#   1. Tasks of a same groups need be processed in order.
#   2. Each worker processes tasks sequentially.
#   3. Worker spawns child process (e.g., a shell script) to handle the real job.
#
# Copyright 2012, Baoshan Sheng, Released under the MIT License

# ## Fairy Explained

# `Fairy` depends on:
#
#   + **[node-uuid]**, generate an unique identifier for each task.
#   + **[redis]**, the node.js driver for Redis, of course!
#   + **[express]**, only if you need the http api or web front-end.
#
# [node-uuid]: https://github.com/broofa/node-uuid
# [redis]:     https://github.com/mranney/node_redis
# [express]:   https://github.com/visionmedia/express
uuid  = require 'node-uuid'
redis = require 'redis'

# A constant prefix will be applied to all Redis keys for safety and
# ease-of-management reasons.
prefix = 'FAIRY'

# ### CommonJS Module Definition

# The only exposed object is a `connect` method, which returns a `fairy` client
# on invocation.
#
#     fairy = require('fairy').connect()
#
# `connect` method use the passed-in option to create a Redis client. Then use
# that Redis client to initiate a new object of class `Fairy`.
exports.connect = (options = {}) ->
  client = redis.createClient options.port, options.host
  client.auth options.password if options.password?
  new Fairy client

# ## Class Fairy

# Class `Fairy` is not exposed outside the commonjs module. To get an object
# of class `Fairy`, use the `connect` method to connect to the Redis server.
#
#     fairy = require('fairy').connect()
#
# Object of class `Fairy` keeps a Redis connection and a pool of named queues
# (objects of class `Queue`) responsible for enqueuing and dispatching tasks,
# etc.
class Fairy

  # ### Constructor

  # The constructor of class `Fairy` stores the passed-in Redis client as an
  # instance property.
  #
  # A `queue_pool` caches named queued as a hashtable. Keys are names of queues,
  # values are according objects of class `Queue`.
  constructor: (@redis) -> @queue_pool = {}

  # ### Function to Resolve Key Name

  # **Private** method to generate prefixed keys. Keys used by objects of class `Fairy`
  # include:
  #
  #   + `QUEUES`, Redis set, containing names of all registered queues.
  #
  # The method is designed to be invoked internally.
  key: (key) -> "#{prefix}:#{key}"

  # ### Get a Named Queue
  
  # If the named queue can be found in the `queue_pool` cache, return the cached
  # queue. Otherwise, create an object of class `Queue` using the Redis client
  # and the name of the queue. Add the queue name into the `QUEUES` set for
  # listing purpose.
  #
  #     foo = fairy.queue 'foo'
  queue: (name) ->
    return @queue_pool[name] if @queue_pool[name]
    @redis.sadd @key('QUEUES'), name
    @queue_pool[name] = new Queue @redis, name

  # ### Get All Queues Asynchronously
  
  # Return named queues whose names are stored in the `QUEUES` set.
  #
  #     queues = fairy.queues()
  #     console.log "#{queues.length} queues: ", queues.map (queue) ->
  #       queue.name
  queues: (callback) ->
    @redis.smembers @key('QUEUES'), (err, res) =>
      callback res.map (name) => @queue name

  # ### Get Statistics for All Queues Asynchronously
  
  # `statistics` is an asynchronous method. The only arg of the callback
  # function is an array containing statistics of all queues. The actual dirty
  # work is handed to objects of class `Queue`'s `statistics` method.
  #
  #     fairy.statistics (stats) ->
  #       console.log "Stats of #{stats.length} queues: ", stats
  statistics: (callback) ->
    @queues (queues) ->
      if total_queues = queues.length
        result = []
        queues.forEach (queue, i) ->
          queue.statistics (statistics) ->
            statistics.name = queue.name
            result[i] = statistics
            callback result if callback unless --total_queues
      else callback [] if callback

# ## Class Queue

# Objects of class `Queue` handles:
#
#   + Placing tasks -- `enqueue`
#   + Regist handlers -- `regist`
#   + Reschedule tasks -- `reschedule`
#   + Query status -- `recently_finished_tasks`, `failed_tasks`,
#   `blocked_groups`, `slowest_tasks`, `processing_tasks`, `statistics`, etc.
#
# Class `Queue` is not exposed outside the commonjs module. To get an object of
# class `Queue`, use the `queue` or `queues` method of an object of class
# `Fairy`:
#
#     foo    = fairy.queue 'foo'
#     queues = fairy.queues()
class Queue

  # ### Constructor

  # The constructor of class `Queue` stores the Redis connection and the name
  # of the queue as instance properties.
  constructor: (@redis, @name) ->

    # When the process exits, if there's un-finished task, the `QUEUED` list need
    # be blocked (add the group identifier into the `BLOCKED` set).
    #
    # Also, gracefully shutting down on SIGINT `(Crtl-C)`.
    global.process.on 'exit', => @redis.sadd @key('BLOCKED'), @processing_group if @processing_group
    global.process.on 'SIGINT', => global.process.exit()

  # ### Function to Resolve Key Name

  # **Private** method to generate (`FAIRY`) prefixed and (queue name) suffixed keys. Keys
  # used by objects of class `Queue` include:
  #
  #   + `SOURCE`, Redis list, tasks reside in `SOURCE` when enqueued.
  #   + `QUEUED`, Redis lists, each group has a separate `QUEUED` list, tasks
  #   enter `QUEUED` lists are prepared for processing in a first-come-first-
  #   serve manner.
  #   + `RECENT`, Redis list, keeps (limited size) recently finished tasks.
  #   + `FAILED`, Redis list, keeps all failed tasks.
  #   + `SLOWEST`, Redis list, keeps (limited size) tasks take the longest
  #   processing time.
  #   + `BLOCKED`, Redis set, keeps names of blocked group.
  #   + `PROCESSING`, Redis hash, tracks tasks in processing.
  #   + `STATISTICS`, Redis hash, tracks basic statistics for the queue.
  key: (key) -> "#{prefix}:#{key}:#{@name}"

  # ### Configurable Parameters
  #
  # Prototypal inherited parameters which can be overriden by instance
  # properties include:

  #   + Polling interval in milliseconds
  #   + Retry interval in milliseconds
  #   + Maximum times of retries
  #   + Storage capacity for newly finished tasks 
  #   + Storage capacity for slowest tasks
  polling_interval : 5
  retry_delay      : 0.1 * 1000
  retry_limit      : 2
  recent_size      : 10
  slowest_size     : 10
  
  # ### Placing Tasks

  # Tasks will be pushed into `SOURCE` Redis lists:
  # 
  #   + `email` tasks will be queued at `source:email` list.
  #   + Arguments except the optional callback function will be serialized as a
  #   JSON array, **the first argument will be served as the group identifier**
  #   to ensure sequential processing for all tasks of the same group (aka.
  #   first-come-first-serve). Current time is appended at the argument array
  #   for statistics. A callback is optional.
  #
  #     queue.enqueue 'param1', 'param2', -> console.log 'queued!'
  #     queue.enqueue 'param1', 'param2'
  # 
  # No transactions are needed for enqueuing tasks.
  enqueue: (args..., callback) =>
    @redis.hincrby @key('STATISTICS'), 'total', 1
    if typeof callback is 'function'
      args.push Date.now()
      @redis.rpush @key('SOURCE'), JSON.stringify(args), callback
    else
      args.push callback
      args.push Date.now()
      @redis.rpush @key('SOURCE'), JSON.stringify(args)

  # ### Register Handler

  # When registering a processing handler function, **Fairy** will immediately
  # start polling tasks and process them on present.
  #
  #     queue.regist (param1, param2, callback) ->
  #       console.log param1, param2
  #       callback()
  regist: (@handler) => @poll()

  # ### Poll New Task

  # If any task presents in the `SOURCE` list, `lpop` from `SOURCE`
  # (`rpush`) into `QUEUED`. The `lpop` and `rpush` are protected by a
  # transaction.
  #
  # Since the task being popped and pushed should be known in prior of the
  # begin of the transaction (aka, the `multi` command), we need to get the
  # first task of the source list, and, watch the source list to prevent the
  # same task being taken by two different workers.
  #
  # If there's no pending tasks of the same group, then process the task
  # immediately.
  #
  # If there's no tasks in the `SOURCE` list, poll again after an interval of
  # `polling_interval` milliseconds.
  poll: =>
    @processing_group = null
    @redis.watch @key('SOURCE')
    @redis.lindex @key('SOURCE'), 0, (err, res) =>
      if res
        task = JSON.parse res
        multi = @redis.multi()
        multi.lpop @key('SOURCE')
        multi.rpush "#{@key('QUEUED')}:#{task[0]}", res
        multi.exec (multi_err, multi_res) =>
          if multi_res and multi_res[1] is 1
            @processing_group = task[0]
            return @process task, on
          @poll()
      else
        @redis.unwatch()
        setTimeout @poll, @polling_interval

  # ### Process First Tasks of Each Group

  # The real job is done by the passed in `handler` of `regist` method, when
  # the job is:
  #
  #   * **successed**, pop the finished job from the group queue, and:
  #     + continue process task of the same group if there's pending job(s) in
  #     the same `QUEUED` list, or
  #     + poll task from the `SOURCE` queue.
  #
  #   * **failed**, then inspect the passed in argument, retry or block
  #   according to the `do` property of the error object.
  #
  #  Calling the callback function is the responsibility of you. Otherwise
  #  `Fairy` will stop dispatching tasks.
  process : (task, is_new_task) =>

    start_time  = Date.now()
    processing = uuid.v4()
    @redis.hset @key('PROCESSING'), processing, JSON.stringify [task..., start_time]
    retry_count = @retry_limit
    errors = []

    call_handler = => @handler task[0...-1]..., (err, res) =>

      # Error handling routine:
      #
      #   1. Keep the error message.
      #   2. According to specific error handling request, when:
      #     + `block`: block the group immediately.
      #     + `block-after-retry`: retry n times, block the group if still
      #     fails (blocking).
      #     + or: retry n times, skip the task if still fails (non-blocking).
      if err
        errors.push err.message or null
        switch err.do
          when 'block'
            @redis.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            @redis.hdel @key('PROCESSING'), processing
            @redis.sadd @key('BLOCKED'), task[0]
            return @poll()
          when 'block-after-retry'
            return setTimeout call_handler, @retry_delay if retry_count--
            @redis.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            @redis.hdel @key('PROCESSING'), processing
            @redis.sadd @key('BLOCKED'), task[0]
            return @poll()
          else
            return setTimeout call_handler, @retry_delay if retry_count--
            @redis.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            @redis.hdel @key('PROCESSING'), processing

      # Success handling routine:
      #
      #   1. Remove last task from processing hash.
      #   2. Update statistics hash:
      #     + total number of `finished` tasks;
      #     + total `pending` time;
      #   3. Track recent finished tasks in `RECENT` list.
      #   4. Track tasks take the longest processing time in `SLOWEST` sorted
      #   set.
      else
        @redis.hdel @key('PROCESSING'), processing
        finish_time  = Date.now()
        process_time = finish_time - start_time
        @redis.hincrby @key('STATISTICS'), 'finished', 1
        @redis.hincrby @key('STATISTICS'), 'total_pending_time', start_time - task[task.length - 1]
        @redis.hincrby @key('STATISTICS'), 'total_processing_time', process_time
        @redis.lpush @key('RECENT'), JSON.stringify([task..., finish_time])
        @redis.ltrim @key('RECENT'), 0, @recent_size - 1
        @redis.zadd @key('SLOWEST'), process_time, JSON.stringify(task)
        @redis.zremrangebyrank @key('SLOWEST'), 0, - @slowest_size - 1

      @continue_group(task[0])

    call_handler()

  # ### Continue Process a Group

  # **Private Method** Upon successful execution of a task, or skipping a
  # failed task:
  #
  #   1. `lpop` the current task from `QUEUED` list.
  #   2. Check if there exists task in the same `QUEUED` list.
  #   3. Process if `YES`, or poll `SOURCE` if `NO`.
  #
  # The above commands need be protected by a transaction. Otherwise, it's
  # possible for 2 workers both processing a same task.
  continue_group: (group) =>
    @redis.watch "#{@key 'QUEUED'}:#{group}"
    multi = @redis.multi()
    multi.lpop "#{@key 'QUEUED'}:#{group}"
    multi.lindex "#{@key 'QUEUED'}:#{group}", 0
    multi.exec (multi_err, multi_res) =>
      if multi_res
        if multi_res[1] then @process JSON.parse(multi_res[1]), true
        else @poll()
      else @continue_group group

  # ### Re-Schedule Failed and Blocked Tasks

  #   1. Requeue tasks in the `FAILED` list into `SOURCE` list, and,
  #   2. Pop all blocked tasks (`QUEUED` lists listed in the `BLOCKED` set,
  #   without first task of each list, because that's the failed task
  #   who blocked the queue which is already requeued in step 1) into `SOURCE`
  #   list.
  #
  # Above commands should be protected by a transaction.
  reschedule: (callback) =>
   
    # Make sure `FAILED` list and `BLOCKED` set are not touched during the
    # transaction.
    @redis.watch @key 'FAILED'
    @redis.watch @key 'BLOCKED'

    # Push all failed tasks (without last two parameters: error message and
    # failure time) into a temporary task array storing tasks to be rescheduled.
    # Then, get all blocked groups.
    @failed_tasks (tasks) =>
      requeued_tasks = []
      requeued_tasks.push tasks.map((task) -> JSON.stringify task[...-2])...
      @blocked_groups (groups) =>

        # Make sure all blocked `QUEUED` list are not touched when you
        # reschedule tasks in them. Then, start the transaction as:
        #
        #   1. Push tasks in the temporary task array into `SOURCE` list.
        #   2. Delete `FAILED` list.
        #   3. Delete all blocked `QUEUED` list.
        #   4. Delete `BLOCKED` set.
        #
        # Commit the transaction, re-initiate the transaction when concurrency
        # occurred, otherwise the reschedule is finished.
        @redis.watch groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
        start_transaction = =>
          multi = @redis.multi()
          multi.rpush @key('SOURCE'), requeued_tasks... if requeued_tasks.length
          multi.del @key 'FAILED'
          multi.del groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
          multi.del @key 'BLOCKED'
          multi.del @key 'PROCESSING'
          multi.exec (multi_err, multi_res) =>
            if multi_res then callback() if callback
            else @reschedule callback

        # If there're blocked task groups, then:
        # 
        #   1. Find all blocked tasks, and:
        #   2. Push them into the temporary tasks array, finally:
        #   3. Start the transaction when this is done for all blocked groups.
        #
        # Otherwise, start the transaction immediately.
        if total_groups = groups.length
          for group in groups
            @redis.lrange "#{@key('QUEUED')}:#{group}", 1, -1, (err, res) =>
              requeued_tasks.push res...
              start_transaction() unless --total_groups
        else start_transaction()

  # ### Get Recently Finished Tasks Asynchronously
  #
  # Recently finished tasks (up to a limited size) will be stored in the
  # `RECENT` list in the reverse order of finished time.
  #
  # **Usage:**
  #
  #     queue.recently_finished_tasks (tasks) ->
  #       console.log "Recently finished tasks are: ", tasks

  # `recently_finished_tasks` is an asynchronous method. The only arg of the
  # callback function will be an array of finished tasks in the reverse order of
  # finished time.
  recently_finished_tasks: (callback) ->
    @redis.lrange @key('RECENT'), 0, -1, (err, res) ->
      callback res.map (entry) -> JSON.parse entry

  # ### Get Failed Tasks Asynchronously
  #
  # Failed tasks are stored in the `FAILED` list.
  #
  # **Usage:**
  #
  #     queue.failed_tasks (tasks) ->
  #       console.log "#{tasks.length} tasks failed: ", tasks

  # `failed_tasks` is an asynchronous method. The only arg of the callback
  # function is an array of failed tasks in the order of failure time.  
  failed_tasks: (callback) ->
    @redis.lrange @key('FAILED'), 0, -1, (err, res) ->
      callback res.map (entry) -> JSON.parse entry

  # ### Get Blocked Groups Asynchronously
  #
  # Blocked groups' identifiers are stored in the `BLOCKED` set.
  #
  # **Usage:**
  #
  #     queue.blocked_groups (groups) ->
  #       console.log "#{groups.length} groups blocked: ", groups

  # `blocked_groups` is an asynchronous method. The only arg of the callback
  # function is an array of identifiers of blocked group. 
  blocked_groups: (callback) ->
    @redis.smembers @key('BLOCKED'), (err, res) ->
      callback res.map (entry) -> JSON.parse entry

  # ### Get Slowest Tasks Asynchronously
  #
  # Slowest tasks are tasks stored in the `SLOWEST` ordered set, which will be
  # limited to a maximum size default to 10.
  #
  # **Usage:**
  #
  #     queue.slowest_tasks (tasks) ->
  #       console.log "Slowest tasks are: ", tasks

  # `slowest_tasks` is an asynchronous method. The only arg of the callback
  # function is an array of slowest tasks in the reverse order by processing
  # time. The actual processing time will be appended at the end of the task's
  # original arguments.
  slowest_tasks: (callback) ->
    @redis.zrevrange @key('SLOWEST'), 0, -1, "WITHSCORES", (err, res) ->
      res = res.map (entry) -> JSON.parse entry
      callback ([res[i]...,res[i + 1]] for i in [0...res.length] by 2)

  # ### Get Currently Processing Tasks Asynchronously
  #
  # Currently processing tasks are tasks in the `RECENT` list, which will be
  # limited to a maximum size defaults to 10.
  #
  # **Usage:**
  #
  #     queue.processing_tasks (tasks) ->
  #       console.log "#{tasks.length} tasks being processing: ", tasks

  # `processing_tasks` is an asynchronous method. The only arg of the callback
  # function is an array of processing tasks of the queue.
  processing_tasks: (callback) ->
    @redis.hvals @key('PROCESSING'), (err, res) ->
      callback res.map (entry) -> JSON.parse entry

  # ### Get Statistics of a Queue Asynchronously
  #
  # Statistics of a queue include:
  # 
  #   + `total_tasks`, total tasks placed
  #   + `finished_tasks`, total tasks finished
  #   + `average_pending_time`, average time spent on waiting for processing the
  #   finished tasks in milliseconds
  #   + `average_processing_time`, average time spent on processing the finished
  #   tasks in milliseconds
  #   + `failed_tasks`, total tasks failed
  #   + `blocked`
  #     - `groups`, total blocked groups
  #     - `tasks`, total blocked tasks
  #   + `pending_tasks`, total pending tasks
  #
  # **Usage:**
  #
  #       queue.statistics (statistics) ->
  #         console.log "Statistics of #{queue.name}:", statistics

  # `statistics` is an asynchronous method. The only arg of the callback
  # function is the statistics of the queue.
  statistics: (callback) ->

    # Start a transaction, in the transaction:
    #
    # 1. Get all fields and values in the `STATISTICS` hash, including:
    #   + `total`
    #   + `finished`
    #   + `total_pending_time`
    #   + `total_processing_time`
    # 2. Get the length of the `FAILED` list (total failed tasks).
    # 3. Get all the members of the `BLOCKED` set (identifiers of blocked group).
    multi = @redis.multi()
    multi.hgetall @key 'STATISTICS'
    multi.hlen @key 'PROCESSING'
    multi.llen @key 'FAILED'
    multi.smembers @key 'BLOCKED'
    multi.exec (multi_err, multi_res) =>

      # Process the result of the transaction.
      #
      # 1. Process `STATISTICS` hash:
      #   + Convert:
      #     - `total_pending_time`, and `total_processing_time` into:
      #     - `average_pending_time`, and `average_processing_time`
      #   + Calibrate initial condition (in case of no task is finished).
      # 2. Set `failed` key of returned object.
      statistics = multi_res[0] or {}
      result =
        total_tasks: statistics.total or 0
        finished_tasks: statistics.finished or 0
        average_pending_time: Math.round(statistics.total_pending_time * 100 / statistics.finished) / 100
        average_processing_time: Math.round(statistics.total_processing_time * 100 / statistics.finished) / 100
      if not result.finished_tasks
        result.average_pending_time = '-'
        result.average_processing_time = '-'
      result.processing_tasks = multi_res[1]
      result.failed_tasks = multi_res[2]

      # Start another transaction to get all `BLOCKED` tasks.
      #
      #   1. Set `blocked.groups` of returned object.
      #   2. Count blocked tasks. Blocked tasks are tasks in the `QUEUED` list whose
      #   group identifier is in the `BLOCKED` set. The first element of each
      #   `QUEUED` list will not be counted, since that's the blocking (already
      #   failed) task.
      #   3. Calculate the pending tasks.
      #
      # The equation used to calculate pending tasks is:
      #
      #     pending = total - finished - processing - blocked - failed
      multi = @redis.multi()
      for group in multi_res[3]
        multi.llen "#{@key 'QUEUED'}:#{group}"
      multi.exec (multi_err2, multi_res2) ->
        result.blocked =
          groups : multi_res[3].length
          tasks : multi_res2.reduce(((a, b) -> a + b), - multi_res[3].length)
        result.pending_tasks = result.total_tasks - result.finished_tasks - result.processing_tasks - result.blocked.tasks - result.failed_tasks
        callback result
