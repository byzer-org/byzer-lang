// 路径配置
require.config({
    paths: {
        echarts: 'http://echarts.baidu.com/build/dist'
    }
});


Date.prototype.Format = function (fmt) { //author: meizz
    var o = {
        "M+": this.getMonth() + 1, //月份
        "d+": this.getDate(), //日
        "h+": this.getHours(), //小时
        "m+": this.getMinutes(), //分
        "s+": this.getSeconds(), //秒
        "q+": Math.floor((this.getMonth() + 3) / 3), //季度
        "S": this.getMilliseconds() //毫秒
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
}



function get(obj, key, defaultValue){

    if(typeof obj[key] != "undefined"){
        return obj[key]
    }else{
        if(typeof defaultValue != "undefined")
            return defaultValue
        else
            return null

    }
}

function attr(obj, key, defaultValue){

    var v = obj.attr(key)
    if(v != null && v != ""){
        return v
    }else{
        return defaultValue
    }
}

function formatData(d, opt){

    var x = null
    var y = []
    var legend = []
    $.each(d, function(i,n){

        var obj = formatOne(n, opt)
        if( x == null)
            x = obj.x

        var yObj = {
            name: n.key,
            type: opt.type,
            //stack: '总量',
            smooth:true,

            data: obj.y
        }

        if(get(opt, "area", false)){

            yObj.itemStyle = {normal: {areaStyle: {type: 'default'}}};
        }

        if(get(opt, "smooth", true)){

            yObj.smooth = true;
        }

        var stack = get(opt, "stack");
        if(stack != null){
            yObj.stack = stack;
        }
        if(get(opt, "average")){
            yObj.markLine = {
                data : [
                    {type : 'average', name: '平均值'}
                ]
            }
        }
        if(get(opt, "markPoint")){
            yObj.markPoint = {
                data : [
                    {type : 'max', name: '最大值'},
                    {type : 'min', name: '最小值'}
                ]
            }
        }




        y.push(yObj)
        legend.push(n.key)

    })

    //保存记录
//            for(s in legend){
//                dataIndex[legend[s]] = 1
//            }

    if(get(opt, "chartType", "line") == "pie"){

        var piey = [];
        $.each(x, function(i,n){

            piey.push({name: n, value: y[0].data[i]})
        })
        y =  [
            {
                name: get(opt, "tip", "数据"),
                type:'pie',
                radius : '55%',
                center: ['50%', '60%'],
                data: piey
            }
        ]
        legend = x;
    }
    return {legend: legend, x: x, y: y}
}
function formatOne(d, opt){

    var x = [];
    var y = [];
    $.each(d.data, function(i, n){

        if(typeof opt.xName != "undefined"){
            x.push(n[opt.xName])
            y.push(n[opt.yName])
        }else{
            x.push(n[opt.xId])
            y.push(n[opt.yId])
        }

    })

    return {x: x, y: y}


}

function format(s, n) {

    if(/^[0-9]+$/.test(s)){
        n = n > 0 && n <= 20 ? n : 2;
        s = parseFloat((s + "").replace(/[^\d\.-]/g, "")).toFixed(n) + "";
        var l = s.split(".")[0].split("").reverse(), r = s.split(".")[1];
        t = "";
        for (i = 0; i < l.length; i++) {
            t += l[i] + ((i + 1) % 3 == 0 && (i + 1) != l.length ? "," : "");
        }
        return t.split("").reverse().join("") ;
    }else{
        return s
    }

}

function showTable(d, opt, area){

    area.find("table").remove()

    var table = $('<table class="table  table-striped table-bordered table-hover "></table>')


    area.append(table)

    var head = []
    head.push("<th>序号</th>")


    var pie = get(opt, "chartType", "line") == "pie"

    if(pie){
        head.push("<th>key</th>")
    }else if(typeof opt.xLabel != "undefined")
        head.push("<th>" + opt.xLabel + "</th>")
    else
        head.push("<th>x轴</th>")



    if(pie){
        head.push("<th>数据</th>")
    }else{
        $.each(d.legend, function(i,n){

            head.push("<th>" + n + "</th>")
        })
    }



    table.append("<tr>" + head.join("") + "</tr>")

    //数据

    var columns = []
    $.each(d.y, function(i, n){

        columns.push(n.data)
    })

    var len = columns.length

    var trs = []

    $.each(d.x, function(i, n){

        var tr = "<tr>" +
            "<td>" + (i + 1) + "</td>" +
            "<td>" + n + "</td>";

        if(pie){
            tr += "<td>" + format(columns[0][i].value,0) + "</td></tr>";
        }else{
            for(j = 0; j < len; j++){

                var obj = columns[j]
                var v = "&nbsp;"

                if(obj.length > i)
                    v = format(obj[i],0)
                tr += "<td>" + v + "</td>"

            }

            tr += "</tr>"

        }


        trs.push(tr)

    })

    table.append(trs.join(""))

}


function load(data, opt){
// 使用
    require(
        [
            'echarts',
            'echarts/chart/line',
            'echarts/chart/pie',
            'echarts/chart/tree',
            'echarts/chart/funnel',
            'echarts/chart/bar' // 使用柱状图就加载bar模块，按需加载
        ],
        function (ec) {

            var d = formatData(data, opt);

            var area = get(opt, 'targetObj', $("#" + get(opt, "target")))


            if(get(opt, "chart", true)){
                // 基于准备好的dom，初始化echarts图表

                var myChart = get(opt, "chartObj")

                if(myChart == null){

                    var chartArea = $('<div  style="height: ' + get(opt, "height", "400") +'px" ></div>')
                    area.append(chartArea)

                    var myChart  =  myChart = ec.init(chartArea[0], "macarons")
                    opt.chartObj = myChart

                }



                var option = null

                if(get(opt, "chartType", "line") == "pie"){

                    option  = {
                        title : {
                            text: opt.title,
                            x:'center',
                            show: false
                        },
                        tooltip : {
                            trigger: 'item',
                            formatter: "{a} <br/>{b} : {c} ({d}%)",
                        },
                        legend: {
                            orient : 'vertical',
                            x : 'left',
                            data: d.x
                        },
                        toolbox: {
                            show : false,
                            feature : {
                                mark : {show: true},
                                dataView : {show: true, readOnly: false},
                                magicType : {
                                    show: true,
                                    type: ['pie', 'funnel'],
                                    option: {
                                        funnel: {
                                            x: '25%',
                                            width: '50%',
                                            funnelAlign: 'left',
                                            max: 1548
                                        }
                                    }
                                },
                                restore : {show: true},
                                saveAsImage : {show: true}
                            }
                        },
                        calculable : true,
                        series : d.y
                    };

                }else{

                    option = {
                        title : {
                            text: opt.title,
                            show: false
                        },
                        animation: false,
                        tooltip : {
                            trigger: 'axis',
                        },
                        legend: {
                            data: d.legend,
                            //orient: 'vertical',
                            //x: 'left', y: 'center',
                            textStyle: {fontSize: '9px'}
                        },
                        toolbox: {
                            show : false,
                            feature : {
                                mark : {show: true},
                                dataView : {show: true, readOnly: false},
                                magicType : {show: true, type: ['line', 'bar']},
                                restore : {show: true},
                                saveAsImage : {show: true}
                            }
                        },
                        calculable : true,
                        xAxis : [
                            {
                                type : 'category',
                                boundaryGap : false,
                                data : d.x
                            }
                        ],
                        yAxis : [
                            {
                                type : 'value'

                            }
                        ],
                        series : d.y
                    };
                }




                area.find("div").show()

                // 为echarts对象加载数据
                myChart.setOption(option);
            }else{

                area.find("div").hide()


            }




            if(get(opt, "table", false)){


                showTable(d, opt, area)
                area.find("table").show()
            }else{
                area.find("table").hide()

            }
        }
    );

}

