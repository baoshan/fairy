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

```bash
npm install fairy
```

## Enqueue Tasks

```coffee-script
queue = require('fairy').connect().queue('foo')
# Provide as many parameters as you needed, the last parameter should be a
# callback.
queue.enqueue 'param_1', 'param_2', 'param_3', -> console.log 'queued'
```

The first element of the 2nd parameter will be used as the group key.

## Register Task Handler

```coffee-script
queue = require('fairy').connect().queue('foo')
# The registered handler function should keep the same signature as you just
# enqueued, plus the last two argument should be 2 callbacks for success and
# fail. Calling them is your responsibility!
queue.regist (param_1, param_2, param_3, callback) ->
  # Do your work here
  setTimeout ->
    err = null
    res = {}
    callback err, res
  , 5
```

See [example folder] for demos. And also the [annotated source].

[example folder]: https://github.com/baoshan/fairy/tree/master/example
[annotated source]: http://baoshan.github.com/fairy/src/fairy.html
