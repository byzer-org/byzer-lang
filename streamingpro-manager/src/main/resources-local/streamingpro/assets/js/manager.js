function checkModule(appName, name, host, config){
    $.ajax({
        url: "/test/moduleDetail.html",
        data: {name: name, host: host},
        dataType: "json",
        success: function(r){

            createModuleDetail(r, config, host,appName, name)
            if(typeof config.complete != "undefined" && config.complete != null)
                config.complete(r);//reSizeHeight()

        }

    })

}

function createModuleDetail(r, config, host, appName, name){



    //[["search_demo_data","stop"],["search_demo_web","stop"],["search_service","start"]]

    //if(r.length > 0){
    //    $("#appInstalledTrue").css("display","")
    //    $("#appInstalledFalse").css("display","none")
    //}
    var tableId = config.tableId || "moduleTable"
    var table = [];
    table.push('<table class="table  table-striped table-bordered table-hover " id = "' + tableId + '">');

    table.push('    <thead>');
    table.push('        <tr>');
    table.push('            <th style="width:5%">序号</th>');
    table.push('            <th style="width:30%">模块名称</th>');
    table.push('            <th>操作</th>');

    table.push('        </tr>');
    table.push('    </thead>');
    table.push('    <tbody>');
    table.push('    </tbody>');
    table.push('</table>');

    var obj = $("#" + config.targetId); //$("#moduleTable tbody")
    obj.empty();
    obj.append(table.join(""));
    obj = obj.find("tbody");

    $.each(r, function(i, n){

        var arr = []
        arr.push("<tr>")
        arr.push("<td>" + (i + 1) +"</td>")
        arr.push("<td>")
        if(n[1] == "start")
            arr.push("<img src='/image/status_start.png' style='width:16px'/>&nbsp;" + n[0])
        else
            arr.push("<img src='/image/status_stop.png'  style='width:16px'/>&nbsp;" + n[0])

        arr.push("</td>")

        arr.push("<td>")

        if(n[1] == "start"){

            arr.push("<button type=button onclick=\"actionAndLog('/app/manager/module.html?action=stop" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'stop','" +  n[0] +"', 'log_" + (i + 1)+"','" + name + "','" + host +"')\" class='btn btn-default' title='停止'><i class='glyphicon glyphicon-stop'></i></button>")
            arr.push("<button type=button onclick=\"actionAndLog('/app/manager/module.html?action=stop_9" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'stop_9','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn btn-danger\" title=\"强制停止\"><i class='glyphicon glyphicon-stop'></i></button>")
            arr.push("<button type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=restart" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'all','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn  btn-default\" title=\"重启\"><i class='glyphicon glyphicon-repeat '></i></button>")

            arr.push("<button type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=all" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'all','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn  btn-default\" title=\"更新并重启\"><i class='glyphicon glyphicon-refresh '></i></button>")

            arr.push("<button type='button' style='height: 24px;padding-top:2px;margin-top:0px' type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=pid&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'pid','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn btn-info\" value=\"PID\">PID</button>")

        }else{

            arr.push("<button type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=start" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'start','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn btn-default\" title=\"启动111\"><i class='glyphicon glyphicon-play'></i></button>")
            arr.push("<button type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=compile" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'compile','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn  btn-default\" title=\"编译\"><i class='glyphicon glyphicon-random'></i></button>")
            arr.push("<button type=\"button\" onclick=\"actionAndLog('/app/manager/module.html?action=compile-u" + "&appName=" + appName + "&aliasName=" + name + "&slaves=" + host + "&module=" + n[0]+"', 'compile-u','" +  n[0] +"', 'log_" + (i + 1)+"','"  + name + "','" + host +"')\" class=\"btn btn-info\" title=\"编译-U\"><i class='glyphicon glyphicon-random'></i></button>")

        }

        var log_file = "logs/" + n[0] + ".log";
        if(n.length == 3){
            log_file =  n[2];
        }
        arr.push("<button id = \"log_" + ( i + 1)+"\" type=\"button\" onclick=\"log('" + name + "','" + host + "', '" + log_file +"', 'area', true, 'log_" + (i + 1)+"')\" class=\"btn  btn-default\" title=\"查看日志\"><i class='glyphicon glyphicon-file'></i></button>")


        arr.push("</td>")
        arr.push("</tr>")
        obj.append(arr.join("\n"))


    })


}


function checkModuleForOneTable(name, host, config){
    $.ajax({
        url: "/test/moduleDetail.html",
        data: {name: name, host: host},
        dataType: "json",
        success: function(r){


            var table = []

            var tbody = null
            if($("#moduleTable").size()  ==  0){

                table.push('<table class="table  table-striped table-bordered table-hover " id = "moduleTable">');

                table.push('    <thead>');
                table.push('        <tr>');
                table.push('            <th style="width:5%">序号</th>');
                table.push('            <th style="width:10%">host</th>');
                $.each(r, function(i, n){
                    table.push('            <th >' + n[0] +'</th>');

                })

                table.push('        </tr>');
                table.push('    </thead>');
                table.push('    <tbody>');
                table.push('    </tbody>');
                table.push('</table>');

                var obj = $("#" + config.targetId); //$("#moduleTable tbody")
                obj.empty();
                obj.append(table.join(""));
                tbody = obj.find("tbody");
            }else{
                tbody = $("#moduleTable tbody")
            }



            var tr = []
            tr.push("<tr>")
            tr.push("<td>" + config.id +"</td>")
            tr.push("<td><a href='/test/detail.html?name=" + name +"&slaves=" + host +"'>" + host +"</a></td>")

            $.each(r, function(i, n){

                tr.push("<td>")
                if(n[1] == "start")
                    tr.push("<img src='/image/status_start.png' style='width:16px'/>&nbsp;")
                else
                    tr.push("<img src='/image/status_stop.png'  style='width:16px'/>&nbsp;")

                tr.push("</td>")
            })


            tr.push("</tr>")

            tbody.append(tr.join(""))

            if(typeof config.complete != "undefined" && config.complete != null)
                config.complete(r);//reSizeHeight()

        },
        error: function(r){

            var tbody = $("#moduleTable tbody")

            var tr = []
            tr.push("<tr>")
            tr.push("<td>0</td>")
            tr.push("<td>" + host +"</td>")



            tr.push("<td>操作</td>")

            tr.push("</tr>")

            tbody.append(tr.join(""))
        }

    })

}