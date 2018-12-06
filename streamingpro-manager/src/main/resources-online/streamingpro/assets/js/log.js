//通过taskId获取日志
//config为参数及回调方法
//可选参数
//taskId
//hostName
//targetId: 用来显示log的document id
//complete: 方法,最后调用
//error:    方法,出错时调用
//render:   显示返回日志时调用,可选,默认直接在targetId元素上显示

var count = {};
window.logMaxLen = 100 * 1024;
window.logPeriod = 500;
function showLogByTaskId(config){//documentCallback, requestCallback){


    if(typeof config.offset == "undefined")
        config.offset = -1
    if(typeof config.reqNum == "undefined")
        config.reqNum = 0;
    $.ajax({
        url: "/test/taskLog.html",
        data: {taskId: config.taskId, host: config.hostName, offset: config.offset},
        dataType: "json",
        success: function(r){


                    config.reqNum = config.reqNum + 1;

                    if(config.render == null || typeof config.render == "undefined"){

                        if(r[0].message != ""){
                            if(offset < 0 )
                                $("#" + config.targetId).html(r[0].message)
                            else{
                                var obj = $("#" + config.targetId);
                                var html = obj.html();
                                if(html.length > logMaxLen){
                                    obj.html(html.substring(logMaxLen / 2 , html.length) + r[0].message)
                                }else{
                                    obj.html(html + r[0].message)
                                }
                            }
                        }

                    }else{
                        config.render(r[0], config)
                    }


            //if(config.requestCallback != null && typeof config.requestCallback != "undefined") {
            //
            //    config.requestCallback(r)
            //}

            if(!r[0].status.finished && stop == false){

                var c =(count[r[0].hostName] | 0 ) + 1;
                count[r[0].hostName] = c;
                if(config.targetId != null && typeof config.targetId != "undefined"){

                    config.id = c;
                    if(c % 2 == 0){
                        $("#" + config.targetId).css("border", "1px solid white")
                    }else{
                        $("#" + config.targetId).css("border", "1px solid #ccc")

                    }
                }
                if(r[0].offset > config.offset)
                    config.offset = r[0].offset;

                setTimeout(function(){showLogByTaskId(config)}, logPeriod)
            }else{



                if(r[0].status.isError && config.error != null && typeof config.error != "undefined") {

                    config.error(config)

                    if(config.targetId != null && typeof config.targetId != "undefined") {

                        $("#" + config.targetId).css("border", "1px solid red")
                    }
                }else{

                    if(config.complete != null && typeof config.complete != "undefined") {

                        config.complete(config)

                    }

                    if(config.targetId != null && typeof config.targetId != "undefined") {

                        $("#" + config.targetId).css("border", "1px solid green")
                    }
                }
                stop = true


            }

        }

    })
}


function tail(config){//documentCallback, requestCallback){


    if(typeof config.offset == "undefined")
        config.offset = -1
    //data: {appName: appName, host: host, path: path, type: "json", offset: offset || -1, size: 4000},
    if(config.before != null && typeof config.before != "undefined")
        if(config.before(config) == false) return;

    if(typeof config.reqNum == "undefined")
        config.reqNum = 0;
    $.ajax({
        url: "/appManager/appManagerLogDetail.html",
        data: {appName: config.appName, host: config.host, offset: config.offset, type: "json",path: config.path},
        dataType: "json",
        success: function(r){


                config.reqNum = config.reqNum + 1;


                if(config.render == null || typeof config.render == "undefined"){

                    if(r[0].message != ""){
                        var html = editor.doc.getValue();
                        if(html.length > logMaxLen){
                            html = html.substring(logMaxLen / 2, html.length) + r[0].message
                        }else{
                            html = html + r[0].message
                        }

                        var top =  autoScroll()
                        editor.doc.setValue(html)

                        if(top == -1)
                            editor.scrollTo(-1, editor.getScrollInfo().height)
                        else
                            editor.scrollTo(-1, top)
                    }

                }else{

                    config.render(r[0], config)
                }


            var offset = r[0].offset;
            //if(offset <= 0){
            //    if(config.lastOffset > 0)offset = config.lastOffset;
            //}
            //else{
            //    config.lastOffset = offset;
            //}
            if(offset > config.offset)
                config.offset = offset

            if(stop == false){
                if(config.success != null && typeof config.success != "undefined")
                    config.success(config);
                setTimeout(function(){tail(config)}, logPeriod)

            }


        }

    })
}


//调整日志框高度
function reSizeHeight(id){

    var height = window.screen.availHeight

    if(typeof id == "undefined") id = "moduleTable";
    var obj = $("#" + id)

    if(obj.size() > 0){
        obj = obj[0]

        height = height - (obj.clientHeight + obj.offsetTop + 150)
    }
    editor.setSize(-1,height)

}


var id = 0;
function actionAndLog(reqUrl, action, module, btnId, name, host,data){

    if(stop == false){
        alert("有任务正在执行,请稍等!!");
        return;
    }
    if(typeof data == "undefined")
        data = {}
    editor.doc.setValue("任务开始执行 " + module + " " + action + "\n");
    $.ajax({
        url: reqUrl,
        data: data,
        dataType: "json",
        success: function(r){


            editor.doc.setValue(r.id)

//            var taskId = r.taskId;
//            //[{"task":{"taskId":"e7b6d9fb36377dedf76577a5e474ba39_b855726157f2e2b251756112327b5122","hostName":"cdn70-115"},"offset":189,"message":"/tmp/mammuthus/e7b6d9fb36377dedf76577a5e474ba39_b855726157f2e2b251756112327b5122/e7b6d9fb36377dedf76577a5e474ba39_b855726157f2e2b251756112327b5122.sh: line 5: echo%20123: command not found\n","status":{"finished":true,"isTimeout":false,"isError":true}}]
//
//            if(typeof r.taskId == "undefined"){
//                taskId = r[0].task.taskId
//            }
//            if(taskId != ""){
//
//                stop = false
//                showLogByTaskId({
//                    taskId: taskId,
//                    hostName: host,
//                    action: action,
//                    offset: -1,
//                    id: 0,
//                    render: function(r, config){
//
//                        if(r.message != ""){
////                                    var obj = $("#" + config.targetId);
////                                if(config.offset < 0 )
////                                    html = r.message
////                                else{
//                            var html = editor.doc.getValue();
//                            if(html.length > logMaxLen){
//                                html = html.substring(logMaxLen / 2, html.length) + r.message
//                            }else{
//                                html = html + r.message
//                            }
//                            //}
//
//                            var top =  autoScroll()
//                            editor.doc.setValue(html)
//
//                            if(top == -1)
//                                editor.scrollTo(-1, editor.getScrollInfo().height)
//                            else
//                                editor.scrollTo(-1, top)
//
//                            //var top =  autoScroll()
//                            //editor.doc.setValue(html)
//                            //editor.scrollTo(-1, top)
//                        }
//
//
//                        if(config.reqNum % 2 == 0){
//                            $(".CodeMirror").css("border", "1px solid white")
//                        }else{
//                            $(".CodeMirror").css("border", "1px solid green")
//
//                        }
//                    },
//                    error: function(){
//
//                        $(".CodeMirror").css("border", "1px solid red")
//
//                    },
//                    complete: function(config){
//
//                        var action = config.action;
//                        if(action == "start" || action == "stop" ||
//                            action == "stop_9" || action == "all"
//                            || action == "install" || action == "update"){
//
//                            check();
//
//                        }
//                        $(".CodeMirror").css("border", "1px solid green")
//
//                        if(action == "start" || action == "all" || action == "restart" ){
//
//                            setTimeout(function(){
//                                log(name, host,'logs/' + module + ".log",null,true, btnId, -1, false)
//                            }, 200)
//
//                        }else{
//                            stop = true;
//                        }
//
//                    }
//                })
//                //showLog("area", r.taskId, "${host}", action)
//            }
        }

    })
}

function autoScroll(){
//            Object {left: 0, top: 308, height: 1123, width: 1153, clientHeight: 800…}
//            clientHeight: 800
//            clientWidth: 1153
//            height: 1123
//            left: 0
//            top: 308
//            width: 1153
    var obj = window.editor.getScrollInfo()
    var viewH = obj.clientHeight;//可见高
    var contentH = obj.height;//内容高度
    var scrollTop = obj.top;//滚动高度
    if(scrollTop == 0 || (contentH - viewH - scrollTop <= 100)) { //到达底部100px时,加载新内容
        //if(scrollTop/(contentH -viewH)>=0.95){ //到达底部100px时,加载新内容
        //return true;
        //return contentH
        return -1
    }else{
        return scrollTop
    }

}
