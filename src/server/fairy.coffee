
# 统计列表的模板数组
arr = [
  '<table class="table table-bordered overview">',
  '<thead><tr><th>Queue</th><th>Workers</th><th>Avg. Time</th><th>Total</th><th>Finished</th><th>Processing</th><th>Pending</th><th>Failed</th><th>Blocked</th><th>Schedule</th><th>Clear</th></tr></thead>',
  '<tbody>',
  '<% _.each(data, function(item){ %>',
  '<tr key=mykey>',
  '<td><%= item.name %></td><td><%= item.workers%></td><td><%= item.average_pending_time%></td><td><span><%= item.total.tasks%></span><span>/</span><span><%= item.total.groups%><span></td><td><%= item.finished_tasks%></td><td><%= item.processing_tasks%></td><td><%= item.pending_tasks%></td><td><%= item.failed_tasks%></td><td><span><%= item.blocked.tasks%></span><span>/</span><span><%= item.blocked.groups%><span></td><td><button class="btn_reschedule">Schedule</button></td><td><button class="btn_clear">Clear</button></td>',
  '</tr>',
  '<%})%>',
  '<tr>',
  '<td>Total</td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.workers); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.average_pending_time); }, 0)%></td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.groups); }, 0) %></span></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.finished_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.processing_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.pending_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.failed_tasks); }, 0)%></td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.groups); }, 0)%></span></td><td>&nbsp;</td><td>&nbsp;</td>',
  '</tr>',
  '</tbody>',
  '</table>'
]

# 用以当队列信息发生变化，更新统计列表的total行
statistics = []

# 下拉列表选中索引
select_index = 0

# timerID：方便终止那段被调用的计时函数 
timer_id = 0

# 初始方法
init = () ->
  console.log (new Date).toString()
  $.ajax({
    type: 'GET'
    url: '/api/queues/statistics'
    success: (data) ->
      statistics = data
      $('#m_statistics').html _.template(arr.join(''), { data : data})
      if $('#queque_detail').is(":visible")
        $($('#m_statistics').find('tr')[select_index]).attr("id","active")
        name = $($($('#m_statistics').find('tr')[select_index]).find('td:first')).html()
        detail_bind name
      select_value = $("select").find("option:selected").text()
      timer_id = setTimeout (-> init()), select_value.substring(0, select_value.length-1)*1000
  })

# 页面加载事件
$(document).ready -> 
  $('select').find("option:nth-child(1)").attr("selected","true")
  $('#queque_detail').hide()
  init()
  bind()

# 注册reschedule和clear事件
bind = () ->
  $('#m_statistics tr[key=mykey]').live 'click', () ->
    $($('#m_statistics').find('tr')[select_index]).removeAttr('id')
    $(this).attr("id","active")
    name = $($(this).find('td')[0]).html()
    select_index = $(this).parent().index()
    detail_bind name
    $('#queque_detail').show()

  ['reschedule', 'clear'].map (command)->
    do (command) ->
      $("#m_statistics .btn_#{command}").live 'click', (event)-> 
        event.stopPropagation()  
        name = $(@).parent().parent().find('td:first').html()
        that = @
        $.ajax({
          type: 'POST'
          url: '/api/queues/' + name + "/#{command}"
          success: (result) ->
            index = $(that).parent().parent().index()
            $(that).parent().parent().html _.template(arr[5], { item: result })
            statistics[index] = result
            $('#m_statistics tr:last').html _.template(arr[9], { data: statistics })
        })

# 单个队列的所有相关明细（统计信息、最近处理任务、失败任务汇总、用时最长任务、处理中任务、工人）数据的绑定
detail_bind = (name)->
  [
    'statistics'
    'recently_finished_tasks'
    'failed_tasks'
    'slowest_tasks'
    'processing_tasks'
    'workers'
  ].map (command)->
    do (command) ->
      $.ajax({
        type: 'GET'
        url: '/api/queues/' + name + "/#{command}"
        success: (results) ->
          param = {}
          param[command] = results
          $("#s_#{command}").html _.template($("#tb_#{command}_template").html(), param)
          if command is 'failed_tasks'
            $(".failed_popover").find(".nav-tabs>li:first").addClass("active")
            $(".failed_popover").find(".tab-content>div:first").addClass("active")
      })

# 下拉列表改变事件
$("select").change () ->
  clearTimeout(timer_id)
  init()

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

