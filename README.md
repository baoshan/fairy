## Redis Queue Battles Message Groups!

**Fairy** is a lightweight queue engine for node.js based on Redis. **Fairy**
offers ActiveMQ's **[message groups]** alike feature which can guarantee
the sequential processing order of tasks belong to a same group.

[Message Groups]: http://activemq.apache.org/message-groups.html

But, unkile **message groups**, **Fairy** doesn't always route tasks of a
group to a same worker, which can lead to unwanted waiting time when:

  1. Tasks of group `X` and `Y` are appointed to worker `A`.
  2. Worker `A` is processing tasks of group `X` **sequentially**.
  3. Tasks of group `Y` are pending, while:
  4. Worker `B` is still idling because of 1.

**Fairy** will route the task of group `Y` to worker `B` in this scenario.

**Fairy** takes a different approach than Message Groups. Instead of making
all tasks of a same group be routed to the same consumer, **Fairy** route a
task to any worker when there's no **processing** tasks of the same group.

The design philosophy makes **Fairy** ideal for the following requirements:

  1. Tasks of a same groups need be processed in sequence.
  2. Each worker processes tasks in serial.
  3. Worker spawns child process (e.g., a shell script) to handle the real job.

**Fairy** takes a different approach than Message Groups. Instead of making all
tasks of a same group be routed to the same consumer, **Fairy** route a task to
any worker when there's no **processing** tasks of the same group.

**[Resque]** cannot guarantee the processing order of the tasks although the task
queue is FIFO. The more workers you have, the more possible you'll encountering
concurrency which breaks the processing order of tasks in the same group.

[Resque]: https://github.com/defunkt/resque

## Installation

    npm install fairy

## Enqueue Tasks

Provide as many parameters as you want, and an optional callback function.
The first argument will be used for message grouping.

    queue = require('fairy').connect().queue('task_name')
    queue.enqueue 'foo', 'bar', ->
      console.log 'more tasks please, sir'

## Register Task Handler

When registered a task handler, the **Fairy** queue becomes a worker
automatically.

The registered handler function will be called when there're tasks to be
processed, with the enqueued parameters. The last argument will be a callback
function. Arguments of the callback function follow node.js error handling
convention: `err` and `res`.

Calling the callback function is your responsibility (or **Fairy** will not
dispatch tasks to the worker and block tasks of the same group forever!)

    queue = require('fairy').connect().queue('task_name')
    queue.regist (param1, param2, callback) ->
      # Do your work here, whether synchronous or asynchronous.
      callback err, res

## Web Front-End

**Fairy** comes with a web front-end. Use it as a express/connect middleware:

    app = require('express').createServer()
    fairy_web = require 'fairy/web'
    app.use fairy_web.middleware
    app.listen 3000

## More APIs

More APIs including:

Objects of Class `Queue`:

+ Placing tasks -- `enqueue`
+ Regist handlers -- `regist`
+ Reschedule tasks -- `reschedule`
+ Query status --
  - `recently_finished_tasks`
  - `failed_tasks`
  - `blocked_groups`
  - `slowest_tasks`
  - `processing_tasks`
  - `workers`
  - `statistics`, etc.

Objects of Class `Fairy`:

+ `queues`, return all queues.
+ `statistics`, return statistics of all queues.

See **[example folder]** for demos. Or explorer the **[annotated source]**.

[example folder]:   https://github.com/baoshan/fairy/tree/master/example
[annotated source]: http://baoshan.github.com/fairy/src/fairy.html
