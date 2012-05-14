(function() {
  var arr, bind, btn_click, data_bind_detail, statistics;

  arr = ['<table class="table table-bordered overview">', '<thead><tr><th>Queue</th><th>Workers</th><th>Avg. Time</th><th>Total</th><th>Finished</th><th>Processing</th><th>Pending</th><th>Failed</th><th>Blocked</th><th>Schedule</th><th>Clear</th></tr></thead>', '<tbody>', '<% _.each(data, function(item){ %>', '<tr key=mykey>', '<td><%= item.name %></td><td><%= item.workers%></td><td><%= item.average_pending_time%></td><td><span><%= item.total.tasks%></span><span>/</span><span><%= item.total.groups%><span></td><td><%= item.finished_tasks%></td><td><%= item.processing_tasks%></td><td><%= item.pending_tasks%></td><td><%= item.failed_tasks%></td><td><span><%= item.blocked.tasks%></span><span>/</span><span><%= item.blocked.groups%><span></td><td><button class="btn_schedule">Schedule</button></td><td><button class="btn_clear">Clear</button></td>', '</tr>', '<%})%>', '<tr>', '<td>Total</td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.workers); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.average_pending_time); }, 0)%></td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.total.groups); }, 0) %></span></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.finished_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.processing_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.pending_tasks); }, 0)%></td><td><%= _.reduce(data, function(memo, item){ return memo + Number(item.failed_tasks); }, 0)%></td><td><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.tasks); }, 0)%></span><span>/</span><span><%= _.reduce(data, function(memo, item){ return memo + Number(item.blocked.groups); }, 0)%></span></td><td><button class="btn_schedule">Schedule</button></td><td><button class="btn_clear">Clear</button></td>', '</tr>', '</tbody>', '</table>'];

  statistics = [];

  $.ajax({
    type: 'GET',
    url: '/api/queues/statistics',
    success: function(data) {
      statistics = data;
      $('#statistics').html(_.template(arr.join(''), {
        data: data
      }));
      return bind();
    }
  });

  btn_click = function(obj) {
    var name;
    console.log(obj);
    name = $(obj).parent().parent().find('td:first').html();
    console.log(name);
    return $.ajax({
      type: 'POST',
      url: '/api/queues/' + name + '/reschedule',
      success: function(stat) {
        var index;
        index = $(obj).parent().parent().index();
        $(obj).parent().parent().html(_.template(arr[5], {
          item: stat
        }));
        statistics[index] = stat;
        return $('#statistics tr:last').html(_.template(arr[9], {
          data: statistics
        }));
      }
    });
  };

  bind = function() {
    $('#statistics tr[key=mykey]').live('click', function() {
      var name, that;
      that = this;
      $('#statistics tr').removeAttr('id');
      $(that).attr("id", "active");
      name = $($(that).find('td')[0]).html();
      console.log(name);
      data_bind_detail(name);
      return $('#queque_detail').show();
    });
    $('#statistics .btn_schedule').live('click', function(event) {
      var name, that;
      event.stopPropagation();
      name = $(this).parent().parent().find('td:first').html();
      that = this;
      console.log(name);
      return $.ajax({
        type: 'POST',
        url: '/api/queues/' + name + '/reschedule',
        success: function(stat) {
          var index;
          index = $(that).parent().parent().index();
          console.log(index);
          $(that).parent().parent().html(_.template(arr[5], {
            item: stat
          }));
          statistics[index] = stat;
          $('#statistics tr:last').html(_.template(arr[9], {
            data: statistics
          }));
          return data_bind_detail(name);
        }
      });
    });
    return $('#statistics .btn_clear').live('click', function(event) {
      var name, that;
      event.stopPropagation();
      name = $(this).parent().parent().find('td:first').html();
      console.log(name);
      that = this;
      return $.ajax({
        type: 'POST',
        url: '/api/queues/' + name + '/clear',
        success: function(stat) {
          var index;
          index = $(that).parent().parent().index();
          console.log(index);
          $(that).parent().parent().html(_.template(arr[5], {
            item: stat
          }));
          statistics[index] = stat;
          $('#statistics tr:last').html(_.template(arr[9], {
            data: statistics
          }));
          return data_bind_detail(name);
        }
      });
    });
  };

  data_bind_detail = function(name) {
    $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/statistics',
      success: function(stat) {
        return $('#statistic').html(_.template($('#tb_statistic_template').html(), {
          statistic: stat
        }));
      }
    });
    $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/recently_finished_tasks',
      success: function(task) {
        return $('#recently_finished_tasks').html(_.template($('#tb_recently_finished_tasks_template').html(), {
          finished_tasks: task
        }));
      }
    });
    $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/failed_tasks',
      success: function(task) {
        return $('#failed_tasks').html(_.template($('#tb_failed_template').html(), {
          failed_tasks: task
        }));
      }
    });
    $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/slowest_tasks',
      success: function(task) {
        return $('#slowest_tasks').html(_.template($('#tb_slowest_template').html(), {
          slowest_tasks: task
        }));
      }
    });
    $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/processing_tasks',
      success: function(task) {
        return $('#processing_tasks').html(_.template($('#tb_processing_tasks_template').html(), {
          processing_tasks: task
        }));
      }
    });
    return $.ajax({
      type: 'GET',
      url: '/api/queues/' + name + '/workers',
      success: function(workers) {
        return $('#workers').html(_.template($('#tb_workers_template').html(), {
          workers: workers
        }));
      }
    });
  };

  $('#queque_detail').hide();

  this.parse_milliseconds = function(milli) {
    var second;
    second = milli / 1000;
    if (second < 1) return milli + 'ms';
    if ((1 < second && second < 60)) return Math.floor(second) + 's';
    if (second > 60) {
      return Math.floor(second / 60) + 'm' + ':' + Math.floor(second % 60) + 's';
    }
  };

}).call(this);
