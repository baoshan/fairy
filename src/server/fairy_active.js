$('.test').hover(
function(){
  $('.test').popover('toggle');
})
$('.test1').hover(
function(){
  $('.test1').tooltip('toggle');
})

//--------------------------Table Crosshair--START----------------------------
function hoverOver()
{
    this.parentNode.className = "hoverRow";

    var rowElements = this.parentNode.parentNode.childNodes;
    // Check in which column the this cell object is at the moment.
    var column = 0;
    var o = this;
    while (o = o.previousSibling) column++;
    for (var row = 0; row < rowElements.length; row++)
    {
        if (rowElements[row].nodeType != 1) continue;
        rowElements[row].childNodes[column].className = "hoverColumn";
    }

    this.className = "hoverCell";
}

function hoverOut()
{
    this.parentNode.className = "";

    var rowElements = this.parentNode.parentNode.childNodes;
    // Check in which column the this cell object is at the moment.
    var column = 0;
    var o = this;
    while (o = o.previousSibling) column++;
    for (var row = 0; row < rowElements.length; row++)
    {
        if (rowElements[row].nodeType != 1) continue;
        rowElements[row].childNodes[column].className = "";
    }
}

function init()
{
    var rowElements = document.getElementsByTagName("tr");
    for (var row = 0; row < rowElements.length; row++)
    {
        columnElements = rowElements[row].childNodes;
        for (var column = 0; column < columnElements.length; column++)
        {
            columnElements[column].onmouseover = hoverOver;
            columnElements[column].onmouseout = hoverOut;
        }
    }
}

window.onload=init;
//--------------------------Table Crosshair--end----------------------------


/*点击图标切换 统计 下显示表格的方式*/
$('.icon-th').click( function(){

  $('.tabbable').addClass('xz');
  $(this).addClass('active');
  $('.icon-th-large').removeClass('active');
})
$('.icon-th-large').click( function(){

  $('.tabbable').removeClass('xz');
  $(this).addClass('active');
  $('.icon-th').removeClass('active');

})

/*顶部阴影*/
$(document).scroll(function(){
  var scroll_top = $(document).scrollTop();
  if(scroll_top>40){
    $('h1').addClass("h1_shadow");
  }
  else{
    $('h1').removeClass("h1_shadow");
  }
})