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
#   4. Worker `B` is snoozing! *(because of 1)*
#
# **Fairy** will route the task of group `Y` to worker `B` in this scenario.
#
# **Fairy** takes a different approach than Message Groups. Instead of making
# all tasks of a same group be routed to the same consumer, **Fairy** route a
# task to any worker when there's no **processing** tasks of the same group.
#
# The design philosophy makes **Fairy** ideal for the following requirements:
#
#   + Tasks of a same groups need be processed in order.
#   + Each worker processes tasks sequentially.
#
# Copyright © 2012, Baoshan Sheng.
# Released under the MIT License.


# ## Fairy in a Nutshell

# **Fairy** depends on:
#
#   + **[redis]**, node.js driver for Redis, of course!
#   + **[node-uuid]**, generate an unique identifier for each task.
#   + **[express]**, required by the [http api] and [web front-end] middleware.
#
# [redis]:         https://github.com/mranney/node_redis
# [node-uuid]:     https://github.com/broofa/node-uuid
# [express]:       https://github.com/visionmedia/express
# [http api]:      fairy_web.html
# [web front-end]: fairy_web.html
uuid  = require 'node-uuid'
redis = require 'redis'
os    = require 'os'

# A constant prefix will be applied to all Redis keys for safety and
# ease-of-management reasons.
prefix = 'FAIRY'


# ### CommonJS Module Definition

# The only exposed object is a `connect` method, which returns a **Fairy**
# client on invocation. **Usage:**
#
#     fairy = require('fairy').connect()
#
# The `connect` method use the passed-in options object to create a Redis
# client. Then use the Redis client to initiate a new object of class `Fairy`.
# The options object could have below keys:
#
#   + `port`, defaults to `6379`.
#   + `host`, defaults to `127.0.0.1`.
#   + `password`, if the Redis server requires authentication.
#   + `options`, read [node_redis documents] for more detail.
#
# [node_redis documents]: https://github.com/mranney/node_redis
exports.connect = (options = {}) ->
  new Fairy options

# ### Exception / Interruption Handling

# Use `uncaughtException` and `SIGINT` to provide elegant exception and user
# interruption handling.

# Module wide variable to instruct all queues exit after processing current
# task.
exiting = off

# Keep current process's all registered workers an array, rely on this array to
# count cleaned workers on exiting.
registered_workers = []

# Log active workers while waiting for all workers to clean-up.
log_registered_workers = ->
  console.log "\nFairy is waiting for #{registered_workers.length} workers to clean-up before exit:"
  for registered_worker in registered_workers
    worker_info = registered_worker.split '|'
    console.log "  * Client Id: [#{worker_info[0]}], Task: [#{worker_info[1]}]"

cleanup_required = off

# Fairy will enter cleanup mode before exit when:
#
#   + Received `SIGINT`, `SIGHUP`, or `SIGUSR2`.
#   + `uncaughtException` captured.
#
# If there's no registered workers, exit directly.
enter_cleanup_mode = ->
  if registered_workers.length
    log_registered_workers()
    cleanup_required = on
    exiting = on
  else
    return process.exit()

# When `SIGINT` (e.g. `Control-C`), `SIGHUP` or `SIGUSR2` is received,
# gracefully exit by notifying all workers entering cleanup mode and exit after
# all cleaned up.
process.on 'SIGINT', enter_cleanup_mode
process.on 'SIGHUP', enter_cleanup_mode
process.on 'SIGUSR2', enter_cleanup_mode

# When `uncaughtException` captured, **Fairy** can not tell if this is caught by
# the handling function, as well as which queue cause the exception. **Fairy**
# will fail all processing tasks and block the according group.
process.on 'uncaughtException', (err) ->
  console.log 'Exception:', err.stack
  console.log 'Fairy workers will block their processing groups before exit.' if registered_workers.length
  enter_cleanup_mode()

# Say goodbye on exit.
process.on 'exit', ->
  console.log "Fairy cleaned up, exiting..." if cleanup_required

# ## Helper Methods

# ### Get Public IP
#
# **Fairy** embed public IP address of workers' environment in workers' name to
# facilitate management.
server_ip = ->
  for card, addresses of os.networkInterfaces()
    for address in addresses
      return address.address if not address.internal and address.family is 'IPv4'
  return 'UNKNOWN_IP'

# ## Create Redis Client
create_client = (options) ->
  client = redis.createClient options.port, options.host, options.options
  client.auth options.password if options.password?
  client


# ## Class Fairy

# Model wide variable used for allocating an increasing integer `id` for each
# **Fairy** client of current process.
fairy_id = 0

# Object of class `Fairy` keeps a Redis connection and a pool of named queues
# (objects of class `Queue`) responsible for enqueuing and dispatching tasks,
# etc.
class Fairy


  # ### Constructor
  #
  # Class `Fairy` is not exposed outside the commonjs module. To get an object
  # of class `Fairy`, use the `connect` method to connect to the Redis server.
  # **Usage:**
  #
  #     fairy = require('fairy').connect()
  
  # The constructor of class `Fairy` stores the passed-in Redis client as an
  # instance property.
  #
  # A `queue_pool` caches named queued as a hashtable. Keys are names of queues,
  # values are according objects of class `Queue`.
  constructor: (@options) ->
    @redis = create_client options
    @id = fairy_id++
    @queue_pool = {}


  # ### Function to Resolve Key Name

  # **Private** method to generate prefixed keys. Keys used by objects of class `Fairy`
  # include:
  #
  #   + `QUEUES`, Redis set, containing names of all registered queues.
  key: (key) -> "#{prefix}:#{key}"


  # ### Get a Named Queue

  # If the named queue can be found in the `queue_pool` cache, return the cached
  # queue. Otherwise, create an object of class `Queue` using the Redis client
  # and the name of the queue. Add the queue name into the `QUEUES` set for
  # listing purpose. **Usage:**
  #
  #     foo = fairy.queue 'foo'
  queue: (name) ->
    return @queue_pool[name] if @queue_pool[name]
    @redis.sadd @key('QUEUES'), name
    @queue_pool[name] = new Queue @, name


  # ### Get All Queues Asynchronously
  
  # Return named queues whose names are stored in the `QUEUES` set.
  #
  #     queues = fairy.queues()
  #     console.log "#{queues.length} queues: ", queues.map (queue) ->
  #       queue.name
  queues: (callback) =>
    @redis.smembers @key('QUEUES'), (err, res) =>
      return callback err if err
      callback null, res.map (name) => @queue name


  # ### Get Statistics for All Queues Asynchronously
  
  # `statistics` is an asynchronous method. The only arg of the callback
  # function is an array containing statistics of all queues. The actual dirty
  # work is handed to objects of class `Queue`'s `statistics` method.
  #
  #     fairy.statistics (stats) ->
  #       console.log "Stats of #{stats.length} queues: ", stats
  statistics: (callback) =>
    @queues (err, queues) ->
      return callback err if err
      return callback null, [] unless total_queues = queues.length
      result = []
      for queue, i in queues
        do (queue, i) ->
          queue.statistics (err, statistics) ->
            return callback err if err
            result[i] = statistics
            callback null, result if callback unless --total_queues

# ## Class Queue

# Objects of class `Queue` handles:
#
#   + Placing tasks -- `enqueue`
#   + Regist handlers -- `regist`
#   + Reschedule tasks -- `reschedule`
#   + Query status --
#     - `recently_finished_tasks`
#     - `failed_tasks`
#     - `blocked_groups`
#     - `slowest_tasks`
#     - `processing_tasks`
#     - `workers`
#     - `statistics`, etc.
#
# Class `Queue` is not exposed outside the commonjs module. To get an object of
# class `Queue`, use the `queue` or `queues` method of an object of class
# `Fairy`. **Usage:**
#
#     foo    = fairy.queue 'foo'
#     queues = fairy.queues()
class Queue

  # ### Constructor

  # The constructor of class `Queue` stores the Redis connection and the name
  # of the queue as instance properties.
  constructor: (@fairy, @name) ->
    @redis = fairy.redis

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
  #   + `foo` tasks will be queued at `SOURCE:foo` list.
  #   + A callback is optional.
  #   + Arguments except the (optional) callback function will be serialized as
  #   a JSON array.
  #   + **The first argument will be served as the group identifier** to ensure
  #   sequential processing for all tasks of the same group (aka. first-come,
  #   first-serve, first-done). Current time is appended at the argument array
  #   for monitoring purpose.
  #
  # **Usage:**
  #
  #     queue.enqueue 'group_id', 'param2', (err, res) -> # YOUR CODE
  # 
  # A transaction ensures the atomicity.
  enqueue: (args..., callback) =>
    if typeof callback isnt 'function'
      args.push callback
      callback = undefined
    @redis.multi()
      .rpush(@key('SOURCE'), JSON.stringify([uuid.v4(), args..., Date.now()]))
      .sadd(@key('GROUPS'), args[0])
      .hincrby(@key('STATISTICS'), 'TOTAL', 1)
      .exec(callback)

  # ### Register Handler

  # When registered a processing handler function, the queue becomes a worker
  # automatically: **Fairy** will immediately start polling tasks and process
  # them on present.
  #
  # When becomes a worker, **Fairy** will regist an uuid (v4) key in the
  # `WORKERS` hash for the queue, and remove the key on exit. Except for hard
  # termination like `SIGKILL`, monitoring the `WORKERS` hash will give you
  # an overview of online workers. **Usage:**
  #
  #     queue.regist (param1, param2, callback) ->
  #       console.log param1, param2
  #       callback()
  regist: (@handler) =>
    registered_workers.push "#{@fairy.id}|#{@name}"
    worker_id = uuid.v4()
    @redis.hset @key('WORKERS'), worker_id, "#{os.hostname()}|#{server_ip()}|#{process.pid}|#{Date.now()}"

    process.on 'uncaughtException', (err) =>
      if @_handler_callback
        console.log "Worker [#{worker_id.split('-')[0]}] registered for Task [#{@name}] will block its current processing group" 
        @_handler_callback {do: 'block', message: err.stack}
      else
        @_try_exit()

    process.on 'exit', => @redis.hdel @key('WORKERS'), worker_id

    @_poll()

  # ### Poll New Task

  # **Private** method. If any task presents in the `SOURCE` list, `lpop` from
  # `SOURCE` (`rpush`) into `QUEUED`. The `lpop` and `rpush` are protected by
  # a transaction.
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
  _poll: =>
    return @_try_exit() if exiting
    @redis.watch @key('SOURCE')
    @redis.lindex @key('SOURCE'), 0, (err, res) =>
      if res
        task = JSON.parse res
        @redis.multi()
        .lpop(@key('SOURCE'))
        .rpush("#{@key('QUEUED')}:#{task[1]}", res)
        .exec (multi_err, multi_res) =>
          return @_poll() unless multi_res and multi_res[1] is 1
          @_process task
      else
        @redis.unwatch()
        setTimeout @_poll, @polling_interval

  # ### Exit When All Queues are Cleaned Up

  # **Private** method. Wait if there're queues still working, or exit the
  # process immediately.
  _try_exit: =>
    registered_workers.splice registered_workers.indexOf "#{@fairy.id}|#{@name}", 1
    process.exit() unless registered_workers.length
    log_registered_workers()

  # ### Process Each Group's First Task

  # **Private** method. The real job is done by the passed in `handler` of
  # `regist`ered method, when the job is:
  #
  #   + **successed**, pop the finished job from the group queue, and:
  #     - continue process task of the same group if there's pending job(s) in
  #     the same `QUEUED` list, or
  #     - poll task from the `SOURCE` queue.
  #   + **failed**, then inspect the passed in argument, retry or block
  #   according to the `do` property of the error object.
  #
  # Calling the callback function is the responsibility of you. Otherwise
  # `Fairy` will stop dispatching tasks.
  _process: (task) =>

    @redis.hset @key('PROCESSING'), task[0], JSON.stringify([task..., start_time = Date.now()])

    # Before Processing the Task:
    #
    #   1. Keep start time of processing.
    #   2. Set `PROCESSING` hash in Redis.
    #   3. Allow `retry_limit` times of retries.
    processing = task[0]
    retry_count = @retry_limit
    errors = []

    @_handler_callback = handler_callback = (err, res) =>

      @_handler_callback = null

      # Error handling routine:
      #
      #   1. Keep the error message.
      #   2. According to specific error handling request, when:
      #     + `block`: block the group immediately.
      #     + `block-after-retry`: retry n times, block the group if still
      #     fails (blocking).
      #     + or: retry n times, skip the task if still fails (non-blocking).
      #     Set `retry_limit` to `0` to skip failed tasks immediately.
      if err
        errors.push err.message or null
        switch err.do
          when 'block'
            multi = @redis.multi()
            multi.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            multi.hdel @key('PROCESSING'), processing
            multi.sadd @key('BLOCKED'), task[1]
            multi.exec()
            return @_poll()
          when 'block-after-retry'
            return setTimeout call_handler, @retry_delay if retry_count--
            multi = @redis.multi()
            multi.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            multi.hdel @key('PROCESSING'), processing
            multi.sadd @key('BLOCKED'), task[1]
            multi.exec()
            return @_poll()
          else
            return setTimeout call_handler, @retry_delay if retry_count--
            multi = @redis.multi()
            multi.rpush @key('FAILED'), JSON.stringify([task..., Date.now(), errors])
            multi.hdel @key('PROCESSING'), processing
            multi.exec()

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
        multi = @redis.multi()
        multi.hdel @key('PROCESSING'), processing
        finish_time  = Date.now()
        process_time = finish_time - start_time
        multi.hincrby @key('STATISTICS'), 'finished', 1
        multi.hincrby @key('STATISTICS'), 'total_pending_time', start_time - task[task.length - 1]
        multi.hincrby @key('STATISTICS'), 'total_process_time', process_time
        multi.lpush @key('RECENT'), JSON.stringify([task..., finish_time])
        multi.ltrim @key('RECENT'), 0, @recent_size - 1
        multi.zadd @key('SLOWEST'), process_time, JSON.stringify([task..., start_time])
        multi.zremrangebyrank @key('SLOWEST'), 0, - @slowest_size - 1
        multi.exec()

      @_continue_group task[1]

    do call_handler = =>
      @handler task[1...-1]..., (@_handler_callback = handler_callback)

  # ### Continue Process a Group

  # **Private** method. Upon successful execution of a task, or skipping a
  # failed task:
  #
  #   1. `lpop` the current task from `QUEUED` list.
  #   2. Check if there exists task in the same `QUEUED` list.
  #   3. Process if `YES`, or poll `SOURCE` if `NO`.
  #
  # Above commands are protected by a transaction to prevent multiple workers
  # processing a same task.
  _continue_group: (group) =>
    @redis.watch "#{@key('QUEUED')}:#{group}"
    @redis.lindex "#{@key('QUEUED')}:#{group}", 1, (err, res) =>
      if res
        task = JSON.parse res
        @redis.unwatch()
        @redis.lpop "#{@key('QUEUED')}:#{group}"
        return @_requeue_group group if exiting
        @_process task
      else
        @redis.multi()
        .lpop("#{@key('QUEUED')}:#{group}")
        .exec (multi_err, multi_res) =>
          return @_continue_group group unless multi_res   
          return @_try_exit() if exiting
          @_poll()
  

  # ### Requeue Tasks on Exit

  # **Private** method. Before exiting, requeue all tasks in the processing
  # `QUEUED` list back into `SOURCE` list to **prevent blocking** the group.
  #
  # To ensure the correct order of tasks, `lpush` tasks in the `QUEUED` list
  # in their reverse order. The `lrange` and `lpush` is protected by a
  # transaction for atomicity since other workers may still shifting tasks in
  # the `SOURCE` list into `QUEUED` list.
  #
  # When tasks are requeued successfully, `_try_exit` the process.
  _requeue_group: (group) =>
    @redis.watch "#{@key('QUEUED')}:#{group}"
    @redis.lrange "#{@key('QUEUED')}:#{group}", 0, -1, (err, res) =>
      @redis.multi()
      .lpush("#{@key('SOURCE')}", res.reverse()...)
      .del("#{@key('QUEUED')}:#{group}")
      .exec (multi_err, multi_res) =>
        return @_requeue_group group unless multi_res
        return @_try_exit()

  # ### Re-Schedule Failed and Blocked Tasks

  # Requeue the failed and blocked tasks into `SOURCE` list. Useful for failure
  # recovery. `reschedule` will:
  #
  #   1. Requeue tasks in the `FAILED` list into `SOURCE` list, and,
  #   2. Pop all blocked tasks (`QUEUED` lists listed in the `BLOCKED` set,
  #   without first task of each list, because that's the failed task
  #   who blocked the queue which is already requeued in step 1) into `SOURCE`
  #   list.
  #
  # Above commands should be protected by a transaction. `reschedule` is an
  # asynchronous method. Arguments of the callback function follow node.js error
  # handling convention: `err` and `res`. On success, the `res` object will be
  # the same as the `res` object of `statistics` method.
  #
  # **Usage:**
  #
  #     queue.reschedule (err, statistics) -> # YOUR CODE
  reschedule: (callback) =>

    client = create_client @fairy.options

    do reschedule = =>
      # Make sure `FAILED` list and `BLOCKED` set are not touched during the
      # transaction.
      client.watch @key('FAILED')
      client.watch @key('SOURCE')
      client.watch @key('BLOCKED')
      client.watch @key('PROCESSING')
      client.hlen @key('PROCESSING'), (err, res) =>
        if res
          client.unwatch()
          return reschedule()

        # Push all failed tasks (without last two parameters: error message and
        # failure time) into a temporary task array storing tasks to be rescheduled.
        # Then, get all blocked groups.
        @failed_tasks (err, tasks) =>
          requeued_tasks = []
          requeued_tasks.push tasks.map((task) -> JSON.stringify [task.id, task.params..., task.queued.valueOf()])...
          @blocked_groups (err, groups) =>

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
            client.watch groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
            start_transaction = =>
              multi = client.multi()
              multi.lpush @key('SOURCE'), requeued_tasks.reverse()... if requeued_tasks.length
              multi.del @key 'FAILED'
              multi.del groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
              multi.del @key 'BLOCKED'
              multi.exec (multi_err, multi_res) =>
                if multi_err
                  client.quit()
                  return callback multi_err 
                if multi_res
                  client.del @key('RESCHEDULE')
                  client.quit()
                  @statistics callback
                  #=>
                  #  @rescheduling = off
                  #  callback.apply @, arguments if callback
                else
                  reschedule callback

            # If there're blocked task groups, then:
            # 
            #   1. Find all blocked tasks, and:
            #   2. Push them into the temporary tasks array, finally:
            #   3. Start the transaction when this is done for all blocked groups.
            #
            # Otherwise, start the transaction immediately.
            if total_groups = groups.length
              for group in groups
                client.lrange "#{@key('QUEUED')}:#{group}", 1, -1, (err, res) =>
                  requeued_tasks.push res...
                  start_transaction() unless --total_groups
            else start_transaction()


  # ### Get Recently Finished Tasks Asynchronously

  # Recently finished tasks are tasks stored in the `RECENT` list (in the
  # reverse order of finished time), which will be limited to a maximum size
  # default to 10.
  #
  # `recently_finished_tasks` is an asynchronous method. Arguments of the
  # callback function follow node.js error handling convention: `err` and `res`.
  #
  # Below is an example `res` array:
  #
  #     [{ id:       '8c0c3eab-8114-41d6-8808-2ae8615d38b4',
  #        params:   [ 'param1', 'param2' ],
  #        queued:   Sat, 12 May 2012 07:41:33 GMT // Date Object
  #        finished: Sat, 12 May 2012 07:41:59 GMT // Date Object
  #      }, ...]
  #
  # **Usage:**
  #
  #     queue.recently_finished_tasks (err, tasks) -> YOUR CODE
  recently_finished_tasks: (callback) =>
    @redis.lrange @key('RECENT'), 0, -1, (err, res) ->
      return callback err if err
      callback null, res.map (entry) ->
        entry = JSON.parse entry
        id: entry[0]
        params: entry[1..-3]
        finished: new Date entry.pop()
        queued: new Date entry.pop()


  # ### Get Failed Tasks Asynchronously
  
  # Failed tasks are stored in the `FAILED` list.
  #
  # `failed_tasks` is an asynchronous method. Arguments of the callback function
  # follow node.js convention: `err` and `res`.
  # 
  # Below is an example `res` array:
  #
  #     [{ id:     '8c0c3eab-8114-41d6-8808-2ae8615d38b4',
  #        params: [ 'param1', 'param2' ],
  #        queued: Sat, 12 May 2012 07:41:33 GMT // Date Object
  #        failed: Sat, 12 May 2012 07:41:59 GMT // Date Object
  #        reason: [ 'failure reason 1', 'failure reason 2', ...]
  #      }, ...]
  #
  # **Usage:**
  #
  #     queue.failed_tasks (err, tasks) -> YOUR CODE
  failed_tasks: (callback) =>
    @redis.lrange @key('FAILED'), 0, -1, (err, res) ->
      return callback err if err
      callback null, res.map (entry) ->
        entry = JSON.parse entry
        id: entry[0]
        params: entry[1..-4]
        reason: entry.pop()
        failed: new Date entry.pop()
        queued: new Date entry.pop()


  # ### Get Blocked Groups Asynchronously
  
  # Blocked groups' identifiers are stored in the `BLOCKED` set.
  #
  # `blocked_groups` is an asynchronous method. Arguments of the callback
  # function follow node.js async callback pattern: `err` and `res`.
  #
  # Below is an example `res` array:
  #
  #     [ 'group1', 'group2', ...]
  #
  # **Usage:**
  #
  #     queue.blocked_groups (err, groups) -> YOUR CODE
  blocked_groups: (callback) ->
    @redis.smembers @key('BLOCKED'), (err, res) ->
      return callback err if err
      callback null, res.map (entry) ->
        entry = JSON.parse entry


  # ### Get Slowest Tasks Asynchronously

  # Slowest tasks are tasks stored in the `SLOWEST` ordered set, which will be
  # limited to a maximum size default to 10.
  #
  # `slowest_tasks` is an asynchronous method. Arguments of the callback
  # function follow node.js error handling convention: `err` and `res`.
  #
  # Below is an example `res` array:
  #
  #     [{ id:      '8c0c3eab-8114-41d6-8808-2ae8615d38b4',
  #        params:  [ 'param1', 'param2' ],
  #        queued:  Sat, 12 May 2012 07:41:33 GMT // Date Object
  #        started: Sat, 12 May 2012 07:41:39 GMT // Date Object
  #        time:    1876 // time taken in milliseconds
  #      }, ...]
  #
  # **Usage:**
  #
  #     queue.slowest_tasks (err, task) -> YOUR CODE
  #
  # `slowest_tasks` is an asynchronous method. The only arg of the callback
  # function is an array of slowest tasks in the reverse order by processing
  # time. The actual processing time will be appended at the end of the task's
  # original arguments.
  slowest_tasks: (callback) ->
    @redis.zrevrange @key('SLOWEST'), 0, -1, "WITHSCORES", (err, res) ->
      return callback err if err
      res = res.map (entry) -> JSON.parse entry
      callback null, ([res[i]...,res[i + 1]] for i in [0...res.length] by 2).map (entry) ->
        id: entry[0]
        params: entry[1..-4]
        time: entry.pop()
        started: new Date entry.pop()
        queued: new Date entry.pop()


  # ### Get Processing Tasks Asynchronously
  
  # Currently processing tasks are tasks in the `PROCESSING` list.
  #
  # `processing_tasks` is an asynchronous method. Arguments of the callback
  # function follow node.js error handling convention: `err` and `res`.
  #
  # Below is an example `res` array:
  #
  #     [{ id:      '8c0c3eab-8114-41d6-8808-2ae8615d38b4',
  #        params:  [ 'param1', 'param2' ],
  #        queued:  Sat, 12 May 2012 07:41:33 GMT // Date Object
  #        started: Sat, 12 May 2012 07:41:39 GMT // Date Object
  #      }, ...]
  #
  # **Usage:**
  #
  #     queue.processing_tasks (err, tasks) -> YOUR CODE
  processing_tasks: (callback) ->
    @redis.hvals @key('PROCESSING'), (err, res) ->
      return callback err if err
      callback null, res.map (entry) ->
        entry = JSON.parse(entry)
        id: entry[0]
        params: entry[1..-3]
        start: new Date entry.pop()
        queued: new Date entry.pop()



  # ### Get Source Tasks Asynchronously

  # Get tasks in the `SOURCE` list. **NOTE:** Tasks in the `SOURCE` list does
  # **NOT** equal to **pending** tasks! There may be tasks in the `QUEUED` lists
  # need be processed before processing tasks in the `SOURCE` list.
  #
  # Accepted parameters are:
  #
  #   1. `skip` *(optional)*, the number of skipped tasks. Defaults to 0.
  #   2. `take` *(optional)*, the number of tasks need be taken. Defaults to 10.
  #   3. `callback`, the callback function. Arguments of which follows node.js error
  #   handling convention: `err` and `res`.
  #
  # Below is an example `res` array:
  #
  #     [{ id:     '8c0c3eab-8114-41d6-8808-2ae8615d38b4',
  #        params: [ 'param1', 'param2' ],
  #        queued: Sat, 12 May 2012 07:41:33 GMT // Date Object
  #      }, ...]
  #
  # Possible combos of arguments are:
  #
  #   1. `callback`. (leave `skip` defaults to 0, `take` defaults to 10).
  #   2. `skip` and `callback`. (leave `take` defaults to 10).
  #   3. `skip`, `take`, and `callback`.
  #
  # **Usage:**
  #
  #     queue.source_tasks 20, 5, (err, tasks) -> YOUR CODE
  source_tasks: (args..., callback) ->
    skip = args[0] or 0
    take = args[1] or 10
    @redis.lrange @key('SOURCE'), skip, skip + take - 1, (err, res) ->
      callback err if err
      callback null, res.map (entry) ->
        entry = JSON.parse entry
        id: entry[0]
        params: entry[1..-2]
        queued: new Date entry.pop()


  # ### Get Workers Asynchronously

  # Asynchronous method to get all **live** workers of the queue. **Live**
  # workers are registered in the `WORKERS` hash. Values of `WORKERS` hash are
  # in `hostname|ip|pid|since` format.
  #
  # Arguments of the callback function follow node.js error handling convention:
  # `err` and `res`. `res` is an array of live workers. Each worker object have:
  #
  #   + `host`, the host name of the worker machine.
  #   + `ip`, the first external IPv4 address of the worker machine.
  #   + `pid`, the process id of the working process.
  #   + `since`, the born date of the worker.
  #
  # Below is an example of returned workers:
  #
  #     [{
  #        host: 'baoshan',
  #        ip: '192.168.2.7',
  #        pid: 1628
  #        since: Sat, 12 May 2012 07:28:21 GMT // Date Object
  #      }, ...]
  #
  # **Usage:**
  #
  #     queue.workers (err, workers) -> YOUR CODE
  workers: (callback) =>
    @redis.hvals @key('WORKERS'), (err, res) ->
      return callback err if err
      callback null, res.map (entry) ->
        entry = entry.split '|'
        host: entry[0]
        ip: entry[1]
        pid: parseInt entry[2]
        since: new Date parseInt entry[3]


  # ### Clear A Queue
  
  # Remove **all** tasks of the queue, and reset statistics.
  clear: (callback) =>
    @redis.watch @key('SOURCE')
    @redis.keys "#{@key('QUEUED')}:*", (err, res) =>
      return callback err if err
      multi = @redis.multi()
      multi.del @key('GROUPS'), @key('RECENT'), @key('FAILED'), @key('SOURCE'), @key('STATISTICS'), @key('SLOWEST'), @key('BLOCKED'), res...
      multi.hmset @key('STATISTICS'), 'TOTAL', 0, 'finished', 0, 'total_pending_time', 0, 'total_process_time', 0
      multi.exec (err, res) =>
        return callback err if err
        return @clear callback unless res
        @statistics callback


  # ### Get Statistics of a Queue Asynchronously
  
  # Statistics of a queue include:
  # 
  #   + `name`, name of the queue.
  #   + `workers`, total live workers.
  #   + `processing_tasks`, total processing tasks.
  #   + `total`
  #     - `groups`, total groups of tasks.
  #     - `tasks`, total tasks placed.
  #   + `finished_tasks`, total tasks finished.
  #   + `average_pending_time`, average time spent on waiting for processing the
  #   finished tasks in milliseconds.
  #   + `average_process_time`, average time spent on processing the finished
  #   tasks in milliseconds.
  #   + `failed_tasks`, total tasks failed.
  #   + `blocked`
  #     - `groups`, total blocked groups.
  #     - `tasks`, total blocked tasks.
  #   + `pending_tasks`, total pending tasks.
  #
  # `statistics` is an asynchronous method. Arguments of the callback function
  # follow node.js asynchronous callback convention: `err` and `res`.
  #
  # Below is an example of the `res` object:
  #
  #       { name: 'task',
  #         workers: 1,
  #         processing_tasks: 0,
  #         total: { groups: 10, tasks: 20000 },
  #         finished_tasks: 8373,
  #         average_pending_time: 313481,
  #         average_process_time: 14,
  #         failed_tasks: 15,
  #         blocked: { groups: 9, tasks: 11612 },
  #         pending_tasks: 0 }
  #
  # If there're no finished tasks, `average_pending_time` and
  # `average_process_time` will both be string `-`.
  #
  # **Usage:**
  #
  #       queue.statistics (err, statistics) -> # YOUR CODE
  statistics: (callback) ->

    # Start a transaction, in the transaction:
    #
    #   1. Count total groups -- `SCARD` of `GROUPS` set.
    #   2. Get all fields and values in the `STATISTICS` hash, including:
    #     + `total`
    #     + `finished`
    #     + `total_pending_time`
    #     + `total_process_time`
    #   3. Count processing tasks -- `LLEN` of `PROCESSING` list.
    #   4. Count failed task -- `LLEN` of `FAILED` list.
    #   5. Get identifiers of blocked group -- `SMEMBERS` of `BLOCKED` set.
    #   6. Count **live** workers of this queue -- `HLEN` of `WORKERS`.
    multi = @redis.multi()
    multi.scard @key('GROUPS')
    multi.hgetall @key('STATISTICS')
    multi.hlen @key('PROCESSING')
    multi.llen @key('FAILED')
    multi.smembers @key('BLOCKED')
    multi.hlen @key('WORKERS')
    multi.exec (multi_err, multi_res) =>
      return callback multi_err if multi_err

      # Process the result of the transaction.
      #
      # 1. Assign transaction results to result object, and:
      # 2. Convert:
      #   - `total_pending_time` into `average_pending_time`, and:
      #   - `total_process_time` into `average_process_time`
      # 3. Calibrate initial condition (in case of no task is finished).
      statistics = multi_res[1] or {}
      result =
        name: @name
        total:
          groups: multi_res[0]
          tasks: parseInt(statistics.TOTAL) or 0
        finished_tasks: parseInt(statistics.finished) or 0
        average_pending_time: Math.round(statistics.total_pending_time * 100 / statistics.finished) / 100
        average_process_time: Math.round(statistics.total_process_time * 100 / statistics.finished) / 100
        blocked:
          groups: multi_res[4].length
        processing_tasks: multi_res[2]
        failed_tasks: multi_res[3]
        workers: multi_res[5]
      if result.finished_tasks is 0
        result.average_pending_time = '-'
        result.average_process_time = '-'

      # Calculate blocked and pending tasks:
      # 
      #   1. Initiate another transaction to count all `BLOCKED` tasks. Blocked
      #   tasks are tasks in the `QUEUED` lists whose group identifiers are in
      #   the `BLOCKED` set. **Note:** The leftmost task of each `QUEUED` list
      #   will not be counted, since that's the causing (failed) task.
      #   2. Calculate pending tasks.
      #
      # The equation used to calculate pending tasks is:
      #
      #      pending = total - finished - processing - failed - blocked
      multi2 = @redis.multi()
      multi2.llen "#{@key('QUEUED')}:#{group}" for group in multi_res[4]
      multi2.exec (multi2_err, multi2_res) ->
        return callback multi2_err if multi2_err
        result.blocked.tasks = multi2_res.reduce(((a, b) -> a + b), - result.blocked.groups)
        result.pending_tasks = result.total.tasks - result.finished_tasks - result.processing_tasks - result.failed_tasks - result.blocked.tasks
        callback null, result
