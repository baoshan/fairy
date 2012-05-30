
# 统计列表的模板数组
statistics_template = [
  '<table class="table table-bordered overview">',
  '<thead><tr><th>Queue</th><th>Workers</th><th>Avg. Time</th><th>Total</th><th>Finished</th><th>Processing</th><th>Pending</th><th>Failed</th><th>Blocked</th><th>Schedule</th><th>Clear</th></tr></thead>',
  '<tbody>',
  '<% _.each(data, function(item){ %>',
  '<tr>',
  '<td><%= item.name %></td><td><%= item.workers%></td><td><%= item.average_pending_time + item.average_process_time%></td><td><span><%= item.total.tasks%></span><span>/</span><span><%= item.total.groups%><span></td><td><%= item.finished_tasks%></td><td><%= item.processing_tasks%></td><td><%= item.pending_tasks%></td><td><%= item.failed_tasks%></td><td><span><%= item.blocked.tasks%></span><span>/</span><span><%= item.blocked.groups%><span></td><td><button class="btn_reschedule">Schedule</button></td><td><button class="btn_clear">Clear</button></td>',
  '</tr>',
  '<%})%>',
  '</tbody>',
  '<tfoot>',
  '<tr>',
  '<td>Total</td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.workers); }, 0)%></td><td>-</td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.groups); }, 0) %></span></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.finished_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.processing_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.pending_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.failed_tasks); }, 0)%></td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.groups); }, 0)%></span></td><td>&nbsp;</td><td>&nbsp;</td>',
  '</tr>',
  '</tfoot>',
  '</table>'
]

# 用以当队列信息发生变化，更新统计列表的total行
statistics = []

# 下拉列表选中索引
select_index = 0

# 刷新时间间隔
interval = $("select option:selected").val()

# 页面加载事件
$ ->
  $('select').find("option:nth-child(1)").attr("selected","true")
  $('#queque_detail').hide()
  render_master()
  events_bind()

# 渲染队列统计列表信息
render_master = () ->
  console.log (new Date).toString()
  $('button').die("click")
  $.get('/api/queues/statistics', (data) ->
    $('#m_statistics').html _.template(statistics_template.join(''), { data : data})
    if $('#queque_detail').is(":visible")
      $($('#m_statistics tbody tr')[select_index]).attr("class","active")
      name = $($($('#m_statistics tbody tr')[select_index]).find('td:first')).html()
      render_slave name
    setTimeout render_master, interval*1000
  )

# 对指定队列的所有相关明细（统计信息、最近处理任务、失败任务汇总、用时最长任务、处理中任务、工人）数据的渲染
render_slave = (name) ->
  commands = [
    'statistics'
    'recently_finished_tasks'
    'failed_tasks'
    'slowest_tasks'
    'processing_tasks'
    'workers'
  ]
  for command in commands
    do (command) ->
      $.get('/api/queues/' + name + "/#{command}", (results) ->
        param = {}
        param[command] = results
        $("#s_#{command}").html _.template($("#tb_#{command}_template").html(), param)
        if command is 'failed_tasks'
          $(".failed_popover").find(".nav-tabs>li:first").addClass("active")
          $(".failed_popover").find(".tab-content>div:first").addClass("active")
      )

# 注册reschedule和clear事件
events_bind = () ->
  $('#m_statistics').find('tbody tr').live 'click', () ->
    $('#m_statistics tr').removeAttr('class')
    $(this).attr("class","active")
    select_index = $(this).index()
    name = $($(this).find('td')[0]).html()
    render_slave name
    $('#queque_detail').show()

  for command in ['reschedule', 'clear']
    do (command) ->
      $("#m_statistics .btn_#{command}").live 'click', (event)-> 
        event.stopPropagation()  
        name = $(@).parent().parent().find('td:first').html()
        that = @
        $.ajax({
          type: 'POST'
          url: '/api/queues/' + name + "/#{command}"
          success: (result) ->
            $(that).parent().parent().html _.template(statistics_template[5], { item: result })
            index = $(that).parent().parent().index()
            statistics[index] = result
            $('#m_statistics tr:last').html _.template(statistics_template[11], { data: statistics })
        })


# 下拉列表改变事件
$("select").change () ->
  interval = $(@).val()

# 点击图标切换 统计 下显示表格的方式
$('.icon-th').click () ->
  $('#workers + .tabbable').addClass('xz')
  $(this).addClass('active')
  $('.icon-th-large').removeClass('active')

$('.icon-th-large').click () ->
  $('#workers + .tabbable').removeClass('xz')
  $(this).addClass('active')
  $('.icon-th').removeClass('active')

#顶部阴影
$(document).scroll () ->
  scroll_top = $(document).scrollTop()
  if scroll_top > 40
    $('h1').addClass("h1_shadow")
  else
    $('h1').removeClass("h1_shadow")

# 转化毫秒形式
@parse_milliseconds = (milli) ->
  second = milli/1000
  return milli+'ms' if second < 1
  return Math.floor(second)+'s' if(1<second<60)
  Math.floor(second/60)+'m'+':'+Math.floor(second%60)+'s' if second > 60

# 提供页面产生唯一编号
@id_factory = () ->
  i = 0
  return {new: () -> return i++ }

