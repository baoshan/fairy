express     = require 'express'
router      = new express.Router()
_           = require 'underscore'
static_     = express.static __dirname + '/web'
connect = null
module.exports.connect = (options = {}) ->
  connect = new Connect(options)
  router

class Connect
		constructor: (options)->
    @fairy = require('../.').connect(options)
  
		# 按5分钟格式化
  format_date: (time) -> time.valueOf() / (5 * 60 * 1000)

  # 获得吞吐量任务个数图形数据（task）
  processing_num: (tasks)->
				_(tasks).chain()
    .groupBy(({finished}) => Math.ceil(@format_date(finished)))
    .map((task, key) -> time: key, value: task.length)
    .value()

		# 为画图数据填充空当时间数据
  merge_draw_data: (draw_data, barrels) ->
				_(barrels)
				  .map((task)->
					  	repeat_task = _.findWhere(draw_data, {time: task.time.toString()})
								time: task.time
								value: repeat_task?.value || task.value
						)
  
		# 获得吞吐量任务个数平均值
  processing_avg_num: (tasks, barrels) ->
				avg_data: (_(draw_data = @processing_num(tasks)).reduce ((memo, task)-> memo + task.value), 0) / work_time = @work_sum_time(@time_maxmin(tasks))
				draw_data: @merge_draw_data _(draw_data).map(({time, value}) ->
				  time: time, value: value / minute = if work_time > 5 then 5 else work_time
				), barrels

  # 获得吞吐量任务分组图形数据（group）	
  processing_group: (tasks) ->
				_(tasks).chain()
    .groupBy(({finished}) => Math.ceil(@format_date(finished)) )
    .map((task, key) ->
        time: key,
        value: _(task).chain()
          .groupBy(({params}) -> JSON.stringify(params))
          .size()
          .value()
    )
    .value()

  # 获得吞吐量任务分组平均值
  processing_avg_group: (tasks) ->
    (_(@processing_group(tasks)).reduce ((memo, task)-> memo + task.value), 0) / (@work_sum_time(@time_maxmin(tasks)))

  # 处理、完成时间图形数据
  task_time: (tasks, minuend_date, percent) ->
				_(tasks)
      .chain()
      .sortBy((task) -> task.finished.valueOf() - task[minuend_date].valueOf())
      .initial(Math.ceil(tasks.length * percent))
						.groupBy(({finished}) => Math.ceil(@format_date(finished)))
						.map((val, key) ->
						  time: key
								value: _(val)
								  .reduce(((memo, task) ->
										  memo + task.finished.valueOf() - task[minuend_date].valueOf()
												)
								,0)
								count: val.length
						)
						.value()

		# 处理、完成时间图形数据格式化为每分钟
  time_draw_data: (draw_data, barrels) ->
				@merge_draw_data _(draw_data).map(({time, value, count}) -> time: time, value: value / count), barrels

  # 平均处理、完成时间
  # 参数：1、tasks 需要处理的数据，2、minuend_date 任务起始时，3、percent 去除百分比
		# 当数值个数等于1的时候去百分比为0，避免出现只有1个值的情况他的百分之98为空
  task_avg: (tasks, minuend_date, percent, barrels) ->
				percent = 0 if tasks.length is 1
				avg_data: (_(draw_data = @task_time(tasks, minuend_date, percent)).reduce ((memo, task)->
				  memo + task.value), 0) / Math.ceil(tasks.length *( 1 - percent))
				draw_data: if barrels then @time_draw_data draw_data, barrels else barrels

  # 恢复日期正常毫秒格式
  repair_date: (time) -> parseInt(time) * 5 * 60 * 1000

  # 取得目前工人运行的总时长
  work_sum_time: ({start, finished}) -> (finished - start) / (1000*60)

  # 获取最大最小时间
		time_maxmin: (tasks) ->
				start = _(tasks).min(({start}) => start).start
				finished = _(tasks).max(({finished}) => finished).finished
    {start: start, finished: finished}

		# 获得图形时间轴范围
		get_darw_time: () ->
    begin_time: Math.ceil(@format_date(new Date() - 1000*60*60*24))
    end_time: Math.ceil(@format_date(new Date()))

  # 获得时间区间
  range_time: () ->
    _(_.range(@get_darw_time().begin_time, @get_darw_time().end_time+1, 1)).map((value) -> {value: 0, time: value})

  # 忙碌工人数图形数据
  busy_work: (tasks, barrels) ->
    tasks.forEach (task) =>
      start = task.start.valueOf()
						finished = task.finished.valueOf()
						barrels.forEach (barrel) =>
        prev = @repair_date(barrel.time - 1)
        since = @repair_date(barrel.time)
        if finished < prev
          return false
        else if start < prev and finished >= since
          barrel.value += @repair_date 1
        else if start < prev and finished <= since and finished > prev
          barrel.value += finished - prev
        else if finished > since and start > prev and start <= since
          barrel.value += since - start
        else if start > prev and finished <= since
          barrel.value += finished - start
				_(barrels).map ({value, time}) -> {time: time, value: value / (5 * 60 * 1000)}

		# 忙碌工人平均值和图形数据
		# 因为图形数据中已经做了每5分钟求一个平均，工作总时长返回数据单位为分钟， 所以需要每分钟基础上除以5得到一共工作时长游多少个5分钟
		busy_avg_work: (tasks, barrels) ->
				avg_value: (_(draw_data = @busy_work(tasks, barrels)).reduce ((memo, task)-> memo + task.value), 0) / ((@work_sum_time(@time_maxmin(tasks))) / 5)
    draw_data: draw_data

# 保留小数四舍五入
math_ceil = (number) -> (Math.ceil(number*100))/100

router.use '/', (req, res, next) -> static_ req, res, next

router.use '/statistics', (req, res, next) ->
		connect.fairy.statistics (err, statistics) ->
				console.log statistics
				result = []
				statistics.forEach (value, index) ->
				  connect.fairy.queue(value.name).recently_finished_tasks new Date().getTime() - 1000*60*60*24, (err, tasks) ->
								barrels = connect.range_time()
								processing_num_data = connect.processing_avg_num(tasks, barrels)
						  processing_time_data = connect.task_avg(tasks, 'start', 0, barrels)
								processing_time_perent_data = connect.task_avg(tasks, 'start', 0.02, barrels)
								work_data = connect.busy_avg_work(tasks, barrels)
								result.push  _.extend statistics[index],
								  processing_num_avg: math_ceil processing_num_data.avg_data
										draw_processing_num_avg: processing_num_data.draw_data
										processing_group_avg: math_ceil connect.processing_avg_group(tasks)
										processing_time_avg: math_ceil processing_time_data.avg_data
										draw_processing_time_avg: processing_time_data.draw_data
										finished_time_avg: math_ceil connect.task_avg(tasks, 'queued', 0)['avg_data']
										processing_time_percent_avg: math_ceil processing_time_perent_data.avg_data
										draw_processing_percent_avg: processing_time_perent_data.draw_data
										finished_time_percent_avg: math_ceil connect.task_avg(tasks, 'queued', 0.02)['avg_data']
										busy_work_num_avg: math_ceil work_data.avg_value
										draw_work_avg: work_data.draw_data
								res.send result if index is statistics.length - 1

['recently_finished_tasks', 'failed_tasks', 'blocked_groups', 'workers'].forEach (command)->
  router.use "/detail/:name/#{command}", (req, res, next) ->
		  connect.fairy.queue(req.params.name)[command] (err, statistics) -> res.send statistics

['retry', 'ignore_failed_tasks', 'clear'].forEach (command) ->
  router.use "/:name/#{command}/", (req, res, next) ->
				connect.fairy.queue(req.params.name)[command] (err, result) -> res.send result
