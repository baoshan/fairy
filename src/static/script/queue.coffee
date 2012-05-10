arr = [
  '<table>',
  '<thead><tr><th>名称</th><th>总数</th><th>完成数</th><th>处理中数</th><th>失败数</th><th>BLOCKED</th><th>等待中</th><th>工人</th><th>操作</th></tr></thead>',
  '<tbody>',
  '<% _.each(data, function(item){ %>',
  '<tr key=mykey>',
  '<td><%= item.name %></td><td><%= item.total.tasks%>/<%= item.total.groups%></td><td><%= item.finished_tasks%></td><td><%= item.processing_tasks%></td><td><%= item.failed_tasks%></td><td><%= item.blocked.tasks%>/<%= item.blocked.groups%></td><td><%= item.pending_tasks%></td><td><%= item.workers%></td><td><button class="btn">CLICK</button> </td>',
  '</tr>',
  '<%})%>',
  '<tr>',
  '<td>total</td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.tasks); }, 0)%>/<%= _.reduce(data, function(memo, item){ return memo + Number(item.total.groups); }, 0) %></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.finished_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.processing_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.failed_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.tasks); }, 0)%>/<%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.groups); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.pending_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.workers); }, 0)%></td><td> </td>',
  '</tr>',
  '</tbody>',
  '</table>'
]

statistics = []
$.ajax({
  type: 'GET'
  url: '/api/queues/statistics'
  success: (data) ->
    statistics = data
    $('#statistics').html _.template(arr.join(''), { data : data})
    bind()
})

btn_click = (obj) ->
  console.log obj
  name = $(obj).parent().parent().find('td:first').html()
  console.log name
  $.ajax({
    type: 'POST'
    url: '/api/queues/' + name + '/reschedule'
    success: (stat) ->
      index = $(obj).parent().parent().index()
      $(obj).parent().parent().html _.template(arr[5], { item: stat })
      statistics[index] = stat
      $('#statistics tr:last').html _.template(arr[9], { data: statistics })
  })

bind = () ->
  $('#statistics tr[key=mykey]').live 'click', () ->
    that = this
    name = $($(that).find('td')[0]).html()
    console.log name
    $.ajax({
      type: 'GET'
      url: '/api/queues/' + name + '/statistics'
      success: (stat) ->

        $('#statistic').html _.template($('#tb_statistic_template').html(), { statistic: stat })
    })
    $.ajax({
      type: 'GET'
      url: '/api/queues/' + name + '/recently_finished_tasks'
      success: (task)->
        $('#recently_finished_tasks').html _.template($('#tb_recently_finished_tasks_template').html(), { finished_tasks: task})
    })
    $.ajax({
      type: 'GET'
      url: '/api/queues/' + name + '/failed_tasks'
      success: (task)->
        $('#failed_tasks').html _.template($('#tb_failed_template').html(), { failed_tasks: task })
    })
    $.ajax({
      type: 'GET'
      url: '/api/queues/' + name + '/slowest_tasks'
      success: (task)->
        $('#slowest_tasks').html _.template($('#tb_slowest_template').html(), { slowest_tasks: task })
    })
    $.ajax({
      type: 'GET'
      url: '/api/queues/' + name + '/workers'
      success: (workers)->
        $('#workers').html _.template($('#tb_workers_template').html(), { workers: workers })
    })
    $('#queque_detail').show()

  $('#statistics .btn').live 'click', (event)-> 
    event.stopPropagation()  
    btn_click(@)
    
$('#queque_detail').hide()

@parse_milliseconds = (milli) ->
  second = milli/1000
  return milli+'ms' if second < 1
  return Math.floor(second)+'s' if(1<second<60)
  Math.floor(second/60)+'m'+':'+Math.floor(second%60)+'s' if second > 60


