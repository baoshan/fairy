# 为页面绑定详细列表弹出事件
bind_detail_table = (name, item_id, is_load) ->
  ['recently_finished_tasks', 'failed_tasks', 'blocked_groups', 'workers'].forEach (value) ->
    $("#ctx#{item_id}").append "<div id='#{value}_#{item_id}' class='error_list' ></div>" if is_load
    $("#btn_#{value}_#{item_id}").on 'click', {name: name, value: value, item_id: item_id}, render_detail

# 渲染详细页面数据
render_detail = (event) ->
  {name, value, item_id} = event.data
  $.get "#{path_}/detail/#{name}/#{value}", (result) ->
    param = {}
    param[value] =
      data: result
      id: "#{value}_#{item_id}"
    $("##{value}_#{item_id}").html _.template($("#tb_#{value}_template").html(), param)
    $("##{value}_#{item_id}").show()
    $("##{value}_#{item_id}").find("button[type='button']").on 'click', -> $(@).parent().hide()
    $("##{value}_#{item_id}").find("button[type='action']").each () ->
      $(@).on 'click', -> $.get("#{path_}/#{name}/#{$(@).attr('data-fuc')}", ->
        $("##{value}_#{item_id}").find('button[type=button]').trigger 'click'
        render_master(off, on)
      )

# 声明d3使用变量
margin = {top: 20, right: 20, bottom: 30, left: 50}
width = 381 - margin.left - margin.right
height = 102 - margin.top - margin.bottom
y = d3.scale.linear().range([height, 0])
x = d3.scale.linear().range([0, width])
yAxis = d3.svg.axis()
  .scale(y)
  .orient("left")
  .tickSize(-width)
  .ticks(2)
  .tickFormat((d) -> d)

# 设置x轴显示坐标
set_xAxis = (draw_data) ->
  _(draw_data)
    .chain()
    .groupBy((task) -> ~~(task.time / 12))
    .map((task, key) -> key * 12)
    .filter((time, index) -> moment(time*5*60*1000).format('HH') in ['00', '06', '12', '18'])
    .value()

# 根据数据绘制图形
draw = (draw_data) ->
  x_arr = set_xAxis(draw_data)
  xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .tickValues(x_arr)
    .tickFormat((d) -> moment(d*5*60*1000).format('HH:mm'))
  line = d3.svg.line()
    .x((d) -> x(d.time))
    .y((d) -> y(d.value))
  svg = d3.select(document.createElement('svg'))
  svg_g = svg.attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append('g')
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
  x.domain(d3.extent(draw_data, (d) -> d.time ))
  y.domain(d3.extent(draw_data, (d) -> d.value))
  svg_g.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + height + ")")
    .call(xAxis)
  gy = svg_g.append("g")
    .attr("class", "y axis")
    .call(yAxis)
  gy.selectAll("g").filter((d) -> d)
    .classed("minor", true)
  gy.selectAll("text")
    .attr("x", 4)
    .attr("dy", -4)
  svg_g.append("path")
    .datum(draw_data)
    .attr("class", "line")
    .attr("d", line(draw_data))
  svg

# 创建页面dom节点
# 为最外侧容器context设置宽度
create_dom = (render_data, is_load, fetch_time) ->
  render_data.forEach (item_data, index) ->
    [
      'draw_processing_num_avg',
      'draw_processing_time_avg',
      'draw_processing_percent_avg',
      'draw_work_avg'
    ].forEach (value) ->
      item_data[value] = draw(item_data[value]).node().outerHTML
      item_data.id = index
      if is_load
        $('#context_template').append("<div id='ctx#{item_data.id}' class='graphs'></div>")
        $("#ctx#{item_data.id}").append("<div id='tb_#{item_data.id}'></div>")
        $('#context_template').width $('#context_template >').outerWidth() * render_data.length
        timer() if fetch_time
        $("#tb_#{item_data.id}").html _.template($('#dashboard_template').html().toString(), item_data)
        bind_detail_table(item_data.name, item_data.id, is_load)
  scroll_to current_index()

# 渲染页面统计数据
# 清空模板
render_master = (is_load, fetch_timer) ->
  $.get "#{path_}/statistics", (render_data) -> create_dom render_data, is_load, fetch_timer

# 页面加载事件
$ -> render_master(on, on)

# 定时刷新页面
timer = ->
  clearInterval timer
  setInterval render_master, 20000

# 获得当前展示为第几个仪表盘
current_index = -> $('#context_template').position().left / task_property().context_width

# 获得当前单个仪表盘宽度和仪表盘数量
task_property = ->
  context_width:0 - $('#context_template >').outerWidth(true)
  task_total: $('#context_template >').length

# 控制内容在可视窗口内移动
scroll_to = (index) ->
  $('#context_template').css('left', task_property().context_width * index )
  $('#button_prev')[if index > 0 then 'show' else 'hide']()
  $('#button_next')[if index < task_property().task_total - 2 then 'show' else 'hide']()

# 左右按钮事件
$('#button_prev').click(-> scroll_to(current_index() - 1))
$('#button_next').click(-> scroll_to(current_index() + 1))
