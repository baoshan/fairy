# *Queue System Treats Tasks Fairly.*
#
# **Fairy** is a lightweight queue engine for node.js based on Redis. Fairy
# offers ActiveMQ's **[message groups]** alike feature which can guarantee
# the sequential processing order of tasks belong to a same group.
#
# [Message Groups]: http://activemq.apache.org/message-groups.html
#
# But, unlike **message groups**, **Fairy** doesn't always route tasks of a
# group to a same worker, which will introduce unwanted waiting time when:
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
#   + Multiple workers need be instantiated to increase throughput.
#
# Copyright © 2012 - 2014, Baoshan Sheng.
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
redis   = require 'redis'
uuid    = require 'node-uuid'


# ### Node.js API Dependencies
#
# + `os`      : retrieve worker host name and ip;
# + `domain`  : catch exceptions thrown by workers precisely;
# + `cluster` : determine `master` / `slave` mode and connected processes.
os      = require 'os'
domain  = require 'domain'
cluster = require 'cluster'


# ### Redis Key Prefix
#
# A prefix will be applied to all Redis keys for safety and
# ease-of-management reasons.
prefix  = 'FAIRY'


# ### Module Scope Variables
#
# + all registered workers;
# + `complete` and `progress` callbacks for unfinished tasks;
# + whether the process is shutting down;
# + whether any uncaught error thrown.
workers       = []
callbacks     = {}
shutting_down = off
error         = off


# ### CommonJS Module Definition
#
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
module.exports =
  version: require('../package.json').version
  connect: (options = {}) ->
    new Fairy options


# ## Exception & Soft Kill Handling
#
# Fairy will shut down the process gracefully when:
#
#   + Soft kill signal received;
#   + `uncaughtException` captured;
#   + `error` captured from worker `domain`.


# ### Shut Down Procedure
#
#   1. Send soft kill signal to slave processes;
#   2. When `shut down` multiple times (force shut down), shut down busy worers;
#   3. Shut down idle workers;
#   4. End the process when all workers and all slave processes shut down.
shut_down = (signo = 'SIGTERM') ->

  for id, worker of (cluster.workers or {})
    worker.suicide = on
    worker.process.kill(signo)

  if shutting_down
    return workers
    .filter(({task}) -> task)
    .forEach((worker) -> worker.shut_down(on))
    
  shutting_down = on

  workers
  .filter(({idle}) -> idle)
  .forEach((worker) -> worker.shut_down(on))

  do waiting_to_exit = ->
    if workers.length or Object.keys(cluster.workers or {}).length
      return setTimeout(waiting_to_exit, 10)
    process.exit(if error then 1 else 0)


# ### Soft Kill Signals
#
# When below signals are captured, gracefully exit the program by notifying all
# workers entering cleanup mode and exit after all are cleaned up.
#
# + `SIGINT` (`Control-C`)
# + `SIGHUP`
# + `SIGQUIT`
# + `SIGUSR1`
# + `SIGUSR2`
# + `SIGTERM`
# + `SIGABRT`
[
  'SIGINT'
  'SIGHUP'
  'SIGQUIT'
  'SIGUSR1'
  'SIGUSR2'
  'SIGTERM'
  'SIGABRT'
].forEach (signo) -> process.on(signo, shut_down.bind(@, signo))


# ### Uncaught Exception
#
# **Fairy** CAN tell if an exception is caused by a worker. Here, we only deal
# with exceptions not thrown by a worker.
process.on 'uncaughtException', (err) ->
  console.error err.stack
  error = on
  shut_down()


# ## Helper Methods


# ### First External IPv4 Address
#
# First external IPv4 address will be embedded in workers' names.
server_ip = ->
  for card, addresses of os.networkInterfaces()
    for address in addresses
      return address.address if address.family in ['IPv4'] and not address.internal
  'N/A'


# ## Create Redis Client
#
# Subscriber clients will be cached.
pubsub_clients = {}
create_client = (options, pubsub) ->
  if pubsub
    index = "#{options.port}|#{options.host}"
    return client if client = pubsub_clients[index]
  client = redis.createClient options.port, options.host, options.options
  client.auth options.password if options.password
  if pubsub
    pubsub_clients[index] = client
    channels = ['FAIRY:COMPLETE', 'FAIRY:PROGRESS']
    client.subscribe(channels...)
    client.on 'message', (channel, message) ->
      return unless channel in channels
      message = JSON.parse("#{message}")
      [task_id, message] = message
      switch channel
        when 'FAIRY:COMPLETE'
          callbacks[task_id]?.complete?(message)
          delete callbacks[task_id]
        when 'FAIRY:PROGRESS'
          callbacks[task_id]?.progress?(message)
  client


# ## Class Fairy
#
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
  #
  # The constructor of class `Fairy` stores the passed-in Redis client as an
  # instance property.
  #
  # A `queue_pool` caches named queued as a hashtable. Keys are names of queues,
  # values are according objects of class `Queue`.
  constructor: (@options) ->
    @id         = uuid.v4()
    @redis      = create_client options
    @pubsub     = create_client options, on
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
#   + Retry failed tasks -- `retry`
#   + Query status --
#     - `pending_tasks`
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
    {@redis, @pubsub} = fairy


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
  enqueue: (group) =>
    original_arguments = Array.prototype.slice.call(arguments)
    for i in [0...arguments.length]
      break if typeof arguments[i] in ['function']
    if typeof arguments[i + 0] in ['function']
      callback_enqueued = arguments[i + 0]
      args = original_arguments[1...i]
    else
      args = original_arguments[1...arguments.length]
    if typeof arguments[i + 1] in ['function']
      callback_complete = arguments[i + 1]
    if typeof arguments[i + 2] in ['function']
      callback_progress = arguments[i + 2]
    task_id = uuid.v4()
    callbacks[task_id] =
      complete: callback_complete
      progress: callback_progress

    @redis.multi()
      .rpush(@key('SOURCE'), JSON.stringify([task_id, group, args..., Date.now()]))
      .sadd(@key('GROUPS'), "#{JSON.stringify(group)}")
      .hincrby(@key('STATISTICS'), 'TOTAL', 1)
      .publish(@key('ENQUEUED'), null)
      .exec(callback_enqueued)

  # ### Register Handler

  # When registered a processing handler function, the queue becomes a worker
  # automatically: **Fairy** will immediately start monitoring tasks and process
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
  regist: (handler) =>
    workers.push new Worker @, handler


  # ### Re-Schedule Failed and Blocked Tasks

  # Requeue the failed and blocked tasks into `SOURCE` list. Useful for failure
  # recovery. `retry` will:
  #
  #   1. Requeue tasks in the `FAILED` list into `SOURCE` list, and,
  #   2. Pop all blocked tasks (`QUEUED` lists listed in the `BLOCKED` set,
  #   without first task of each list, because that's the failed task
  #   who blocked the queue which is already requeued in step 1) into `SOURCE`
  #   list.
  #
  # Above commands should be protected by a transaction. `retry` is an
  # asynchronous method. Arguments of the callback function follow node.js error
  # handling convention: `err` and `res`. On success, the `res` object will be
  # the same as the `res` object of `statistics` method.
  #
  # **Usage:**
  #
  #     queue.retry (err, statistics) -> # YOUR CODE
  retry: (callback) =>

    client = create_client @fairy.options

    do retry = =>
      # Make sure `FAILED` list and `BLOCKED` set are not touched during the
      # transaction.
      client.watch @key('FAILED')
      client.watch @key('SOURCE')
      client.watch @key('BLOCKED')
      client.watch @key('PROCESSING')
      client.hlen @key('PROCESSING'), (err, res) =>
        if res
          client.unwatch()
          return retry()

        # Push all failed tasks (without last two parameters: error message and
        # failure time) into a temporary task array storing tasks to be retryd.
        # Then, get all blocked groups.
        @failed_tasks (err, tasks) =>
          requeued_tasks = []
          requeued_tasks.push tasks.map((task) -> JSON.stringify [task.id, task.params..., task.queued.valueOf()])...

          @blocked_groups (err, groups) =>
            # Make sure all blocked `QUEUED` list are not touched when you
            # retry tasks in them. Then, start the transaction as:
            #
            #   1. Push tasks in the temporary task array into `SOURCE` list.
            #   2. Delete `FAILED` list.
            #   3. Delete all blocked `QUEUED` list.
            #   4. Delete `BLOCKED` set.
            #
            # Commit the transaction, re-initiate the transaction when concurrency
            # occurred, otherwise the retry is finished.
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
                  client.quit()
                  @redis.publish(@key('ENQUEUED'), "")
                  @statistics callback
                else
                  retry callback

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


  # ### Clear A Queue
  
  # Remove **all** tasks of the queue, and reset statistics. Set `TOTAL` to
  # `PROCESSING` tasks to prevent negative pending tasks being calculated.
  clear: (callback) =>
    @redis.watch @key('SOURCE')
    @redis.watch @key('PROCESSING')
    @redis.hlen @key('PROCESSING'), (err, processing) =>
      return callback? err if err
      @redis.keys "#{@key('QUEUED')}:*", (err, res) =>
        return callback? err if err
        @redis.multi()
        .del(@key('GROUPS'), @key('RECENT'), @key('FAILED'), @key('SOURCE'), @key('STATISTICS'), @key('SLOWEST'), @key('BLOCKED'), @key('GROUPS:FINISHED'), @key('GROUPS:FAILED'), res...)
        .hmset(@key('STATISTICS'), 'TOTAL', processing, 'FINISHED', 0, 'TOTAL_PENDING_TIME', 0, 'TOTAL_PROCESS_TIME', 0)
        .exec (err, res) =>
          return callback? err if err
          return @clear callback unless res
          @statistics callback if callback


  # ### Ignore Failed Tasks
  
  # Remove **all** tasks of the queue, and reset statistics. Set `TOTAL` to
  # `PROCESSING` tasks to prevent negative pending tasks being calculated.
  ignore_failed_tasks: (callback) =>

    client = create_client @fairy.options

    do retry = =>
      # Make sure `FAILED` list and `BLOCKED` set are not touched during the
      # transaction.
      client.watch @key('FAILED')
      client.watch @key('SOURCE')
      client.watch @key('BLOCKED')
      client.watch @key('PROCESSING')
      client.hlen @key('PROCESSING'), (err, res) =>
        if res
          client.unwatch()
          return retry()

        # Push all failed tasks (without last two parameters: error message and
        # failure time) into a temporary task array storing tasks to be retryd.
        # Then, get all blocked groups.
        # @failed_tasks (err, tasks) =>
        requeued_tasks = []
        # requeued_tasks.push tasks.map((task) -> JSON.stringify [task.id, task.params..., task.queued.valueOf()])...

        @blocked_groups (err, groups) =>
          # Make sure all blocked `QUEUED` list are not touched when you
          # retry tasks in them. Then, start the transaction as:
          #
          #   1. Push tasks in the temporary task array into `SOURCE` list.
          #   2. Delete `FAILED` list.
          #   3. Delete all blocked `QUEUED` list.
          #   4. Delete `BLOCKED` set.
          #
          # Commit the transaction, re-initiate the transaction when concurrency
          # occurred, otherwise the retry is finished.
          client.watch groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
          start_transaction = =>
            multi = client.multi()
            multi.lpush @key('SOURCE'), requeued_tasks.reverse()... if requeued_tasks.length
            multi.del @key 'FAILED'
            multi.del @key 'GROUPS:FAILED'
            multi.del groups.map((group) => "#{@key('QUEUED')}:#{group}")... if groups.length
            multi.del @key 'BLOCKED'
            multi.exec (multi_err, multi_res) =>
              if multi_err
                client.quit()
                return callback multi_err
              if multi_res
                client.quit()
                @redis.publish(@key('ENQUEUED'), "")
                @statistics callback
              else
                retry callback

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


  # ##  Read-Only Operations

  pending_groups: (callback) =>

    @pending_tasks (err, pending_tasks) =>
      return callback err if err
      groups = {}
      for pending_task in pending_tasks
        groups[JSON.stringify(pending_task.params[0])] = 1
      callback null, Object.keys(groups)

  # ##  Read-Only Operations

  pending_tasks: (callback) =>

    @redis.multi()
      .smembers(@key('GROUPS'))
      .lrange(@key('SOURCE'), 0, -1)
      .exec (multi_err, multi_res) =>
        return callback multi_err if multi_err

        pending_tasks = multi_res[1].map (entry) ->
          entry = JSON.parse entry
          id     : entry[0]
          params : entry[1...-1]
          queued : new Date entry.pop()

        multi2 = @redis.multi()
        multi2.lrange("#{@key('QUEUED')}:#{group}", 1, -1) for group in multi_res[0]
        multi2.exec (multi2_err, multi2_res) ->
          return callback multi2_err if multi2_err
          callback null, pending_tasks.concat multi2_res.reduce(((memo, queued) -> memo.concat queued), []).map((entry) ->
            entry    = JSON.parse entry
            id     : entry[0]
            params : entry[1...-1]
            queued : new Date entry.pop())

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
  recently_finished_tasks: (after, callback) =>
    if typeof after in ['function']
      callback = after
      after = undefined
    @redis.lrange @key('RECENT'), 0, -1, (err, res) ->
      return callback err if err
      callback null, res.map((entry) ->
        entry = JSON.parse entry
        id       : entry[0]
        params   : entry[1...-4]
        finished : new Date entry.pop()
        start    : new Date entry.pop()
        queued   : new Date entry.pop()
      ).filter ({finished}) -> not after or finished >= after


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
  failed_tasks: (after, callback) =>
    if typeof after in ['function']
      callback = after
      after = undefined
    @redis.lrange @key('FAILED'), 0, -1, (err, res) ->
      return callback err if err
      callback null, res.map((entry) ->
        entry = JSON.parse entry
        id     : entry[0]
        params : entry[1...-4]
        reason : entry.pop()
        failed : new Date entry.pop()
        start  : new Date entry.pop()
        queued : new Date entry.pop()
      ).filter ({failed}) -> not after or failed >= after


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
      callback null, res.map((entry) ->
        entry = entry.split '|'
        host: entry[0]
        ip: entry[1]
        pid: parseInt entry[2]
        since: new Date parseInt entry[3]
      ).sort (a, b) ->
        return  1 if a.ip  > b.ip
        return -1 if a.ip  < b.ip
        return  1 if a.pid > b.pid
        return -1 if a.pid < b.pid


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
  #   + `averageprocess_time`, average time spent on processing the finished
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
  #         averageprocess_time: 14,
  #         failed_tasks: 15,
  #         blocked: { groups: 9, tasks: 11612 },
  #         pending_tasks: 0 }
  #
  # If there're no finished tasks, `average_pending_time` and
  # `averageprocess_time` will both be string `-`.
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
    #     + `totalprocess_time`
    #   3. Count processing tasks -- `LLEN` of `PROCESSING` list.
    #   4. Count failed task -- `LLEN` of `FAILED` list.
    #   5. Get identifiers of blocked group -- `SMEMBERS` of `BLOCKED` set.
    #   6. Count **live** workers of this queue -- `HLEN` of `WORKERS`.
    @redis.multi()
      .scard(@key('GROUPS'))
      .hgetall(@key('STATISTICS'))
      .hlen(@key('PROCESSING'))
      .llen(@key('FAILED'))
      .smembers(@key('BLOCKED'))
      .hlen(@key('WORKERS'))
      .scard(@key('GROUPS:FINISHED'))
      .scard(@key('GROUPS:FAILED'))
      .exec (multi_err, multi_res) =>
        return callback multi_err if multi_err

        # Process the result of the transaction.
        #
        # 1. Assign transaction results to result object, and:
        # 2. Convert:
        #   - `total_pending_time` into `average_pending_time`, and:
        #   - `totalprocess_time` into `averageprocess_time`
        # 3. Calibrate initial condition (in case of no task is finished).
        statistics = multi_res[1] or {}
        result =
          name: @name
          total:
            groups: multi_res[0]
            tasks: parseInt(statistics.TOTAL) or 0
          finished:
            groups: multi_res[6]
            tasks: parseInt(statistics.FINISHED) or 0
          average_pending_time: Math.round(statistics.TOTAL_PENDING_TIME * 100 / statistics.FINISHED) / 100
          averageprocess_time: Math.round(statistics.TOTAL_PROCESS_TIME * 100 / statistics.FINISHED) / 100
          blocked:
            groups: multi_res[4].length
          processing_tasks: multi_res[2]
          failed:
            groups: multi_res[7]
            tasks: multi_res[3]
          pending: {}
          workers: multi_res[5]
        if result.finished.tasks is 0
          result.average_pending_time = '-'
          result.averageprocess_time = '-'

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
        multi2.exec (multi2_err, multi2_res) =>
          return callback multi2_err if multi2_err
          result.blocked.tasks = multi2_res.reduce(((a, b) -> a + b), - result.blocked.groups)
          result.pending.tasks = result.total.tasks - result.finished.tasks - result.processing_tasks - result.failed.tasks - result.blocked.tasks
          @pending_groups (err, pending_groups) ->
            return callback err if err
            result.pending.groups = pending_groups.length
            callback null, result


# ### Worker Definition

class Worker

  # ### Configurable Parameters
  #
  # Prototypal inherited parameters which can be overriden by instance
  # properties include:

  #   + Polling interval in milliseconds
  #   + Maximum times of retries
  #   + Retry interval in milliseconds
  #   + Storage capacity for newly finished tasks 
  #   + Storage capacity for slowest tasks
  retry_limit      : 2
  retry_delay      : 0
  recent_size      : 100000
  slowest_size     :  10000

  constructor: (@queue, @handler) ->
    {@name, @fairy, @redis, @pubsub} = queue
    @id = uuid.v4()
    @redis.hset @key('WORKERS'), @id, "#{os.hostname()}|#{server_ip()}|#{process.pid}|#{Date.now()}"

    @pubsub.subscribe @key('ENQUEUED')
    @pubsub.on 'message', (channel, message) =>
      return unless channel in [@key('ENQUEUED')]
      @start() if @idle
    @idle = on
    @start()
    
  key: (key) -> "#{prefix}:#{key}:#{@name}"


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
  # If there's no tasks in the `SOURCE` list, take worker into `idle` state.
  start: =>
    return @shut_down() if shutting_down or error
    @idle = off
    @redis.watch @key('SOURCE')
    @redis.lindex @key('SOURCE'), 0, (err, res) =>
      if task = JSON.parse(res)
        @redis.multi()
        .lpop(@key('SOURCE'))
        .rpush("#{@key('QUEUED')}:#{task[1]}", res)
        .exec (multi_err, multi_res) =>
          return @start() unless multi_res?[1] is 1
          @process task
      else
        @redis.unwatch()
        @idle = on


  # ### Process Each Group's First Task

  # **Private** method. The real job is done by the passed in `handler` of
  # `regist`ered method, when the job is:
  #
  #   + **successed**, pop the finished job from the group queue, and:
  #     - continue process task of the same group if there's pending job(s) in
  #     the same `QUEUED` list, or
  #     - pull task from the `SOURCE` queue.
  #   + **failed**, then inspect the passed in argument, retry or block
  #   according to the `do` property of the error object.
  #
  # Calling the callback function is the responsibility of you. Otherwise
  # `Fairy` will stop dispatching tasks.
  process: (@task) =>
    @redis.hset @key('PROCESSING'), @id, JSON.stringify([task..., task.start_time = Date.now()])

    # Before Processing the Task:
    #
    #   1. Keep start time of processing.
    #   2. Set `PROCESSING` hash in Redis.
    #   3. Allow `retry_limit` times of retries.
    #
    retry_count = @retry_limit
    errors = []

    handler_callback = (err, res) =>
      delete @task
      return if @shutting_down

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
            @redis.multi()
            .rpush(@key('FAILED'), JSON.stringify([task..., task.start_time, Date.now(), errors]))
            .sadd(@key('GROUPS:FAILED'), task[1])
            .hdel(@key('PROCESSING'), @id)
            .sadd(@key('BLOCKED'), task[1])
            .exec()
            return @start()
          when 'block-after-retry'
            return setTimeout process_task, @retry_delay if retry_count--
            @redis.multi()
            .rpush(@key('FAILED'), JSON.stringify([task..., task.start_time, Date.now(), errors]))
            .sadd(@key('GROUPS:FAILED'), task[1])
            .hdel(@key('PROCESSING'), @id)
            .sadd(@key('BLOCKED'), task[1])
            .exec()
            return @start()
          else
            return setTimeout process_task, @retry_delay if retry_count--
            @redis.multi()
            .rpush(@key('FAILED'), JSON.stringify([task..., task.start_time, Date.now(), errors]))
            .sadd(@key('GROUPS:FAILED'), task[1])
            .hdel(@key('PROCESSING'), @id)
            .exec()

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
        finish_time  = Date.now()
        process_time = finish_time - task.start_time
        @redis.multi()
        .hdel(@key('PROCESSING'), @id)
        .hincrby(@key('STATISTICS'), 'FINISHED', 1)
        .sadd(@key('GROUPS:FINISHED'), task[1])
        .hincrby(@key('STATISTICS'), 'TOTAL_PENDING_TIME', task.start_time - task[task.length - 1])
        .hincrby(@key('STATISTICS'), 'TOTAL_PROCESS_TIME', process_time)
        .lpush(@key('RECENT'), JSON.stringify([task..., task.start_time, finish_time]))
        .ltrim(@key('RECENT'), 0, @recent_size - 1)
        .zadd(@key('SLOWEST'), process_time, JSON.stringify([task..., task.start_time]))
        .zremrangebyrank(@key('SLOWEST'), 0, - @slowest_size - 1)
        .publish('FAIRY:COMPLETE', JSON.stringify([task[0], res]))
        .exec()

      @continue_group task[1]

    do process_task = =>

      # Create a `domain` to capture exception thrown by handler.
      d = domain.create()

      d.on 'error', (error) =>
        error = on
        console.error error.stack
        handler_callback (do: 'block', message: error.stack)

      d.run =>
        @handler(task[1...-1]..., handler_callback, (progress) => @redis.publish 'FAIRY:PROGRESS', JSON.stringify [task[0], progress])


  # ### Continue Process a Group

  # **Private** method. Upon successful execution of a task, or skipping a
  # failed task:
  #
  #   1. `lpop` the current task from `QUEUED` list.
  #   2. Check if there exists task in the same `QUEUED` list.
  #   3. Process if `YES`, or pull `SOURCE` if `NO`.
  #
  # Above commands are protected by a transaction to prevent multiple workers
  # processing a same task.
  #
  # ### Requeue Tasks on Exit
  #
  # **Private** method. Before exiting, requeue all tasks in the processing
  # `QUEUED` list back into `SOURCE` list to **prevent blocking** the group.
  #
  # To ensure the correct order of tasks, `lpush` tasks in the `QUEUED` list
  # in their reverse order. The `lrange` and `lpush` is protected by a
  # transaction for atomicity since other workers may still shifting tasks in
  # the `SOURCE` list into `QUEUED` list.
  #
  # When tasks are requeued successfully, the worker `unregist` itself.
  continue_group: (group) =>
    @redis.watch "#{@key('QUEUED')}:#{group}"
    @redis.lindex "#{@key('QUEUED')}:#{group}", 1, (err, res) =>
      if task = JSON.parse(res)
        if shutting_down
          @redis.lrange "#{@key('QUEUED')}:#{group}", 1, -1, (err, res) =>
            @redis.multi()
            .lpop("#{@key('QUEUED')}:#{group}")
            .lpush("#{@key('SOURCE')}", res.reverse()...)
            .del("#{@key('QUEUED')}:#{group}")
            .exec (multi_err, multi_res) =>
              return @continue_group(group) unless multi_res
              @start()

        else
          @redis.unwatch()
          @redis.lpop "#{@key('QUEUED')}:#{group}"
          @process(task)

      else
        @redis.multi()
        .lpop("#{@key('QUEUED')}:#{group}")
        .exec (multi_err, multi_res) =>
          return @continue_group(group) unless multi_res
          @start()


  # ### Shut Down Worker
  #
  # + If the worker is busy, `fail` and `block` the current task, clear `processing`;
  # + Unregist the worker;
  # + Remove the worker from registered workers array.
  # + Request the process to be shut down.
  shut_down: (do_not_bubble) ->
    return if @shutting_down
    @shutting_down = on
    if @task
      @redis.hdel(@key('PROCESSING'), @id)
      @redis.rpush(@key('FAILED'), JSON.stringify([@task..., @task.start_time, Date.now(), ['Force shut down manually.']]))
      @redis.sadd(@key('GROUPS:FAILED'), @task[1])
      @redis.sadd(@key('BLOCKED'), @task[1])
      delete @task
    @redis.hdel(@key('WORKERS'), @id)
    workers.splice(workers.indexOf(@), 1)
    shut_down() unless shutting_down or do_not_bubble
