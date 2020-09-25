# 外置调度整合

因为内置调度不够成熟，用户会倾向于使用第三方调度。如前所述，因为MLSQL Console提供了脚本管理以及执行脚本的
能力，并且其功能都是以HTTP的方式暴露出来的，所以我们只要访问console就可完成所有调度能力。


首先，我们需要能够让调度访问Console,因为Console要经过授权才能访问，为了简化，我们允许用户配置一个超级密钥，在
console的application.yml文件里添加如下一行：

```
auth_secret: "your-password"
```

现在，我们就可以访问console获取脚本的接口. 首先，我们通过脚本名字获取脚本id:

```scala
def getScriptId(path: String) = {
      def encode(str: String) = {
        URLEncoder.encode(str, "utf-8")
      }

val script = Request.Get(PathFun(consoleUrl).add(s"/api_v1/script_file/path/id?path=${encode(path)}&owner=${encode(context.owner)}").toPath)
.connectTimeout(60 * 1000)
.socketTimeout(10 * 60 * 1000).addHeader("access-token", authSecret)
.execute().returnContent().asString()
script.toInt
    }
```

脚本名称类似 `demo.algorithm.myscript.mlsql`. 通过该`/api_v1/script_file/path/id?path=&owner=` 接口就能获得该脚本的id.
同时，上面的authSecret就是我们前面配置的`your-password`，并且是配置在请求头里的。获得id后，可以通过如下方式获得脚本的具体内容以及owner等属性

```scala
val script = Request.Get(PathFun(consoleUrl).add(s"/api_v1/script_file/get?id=${job.id}").toPath)
      .connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).addHeader("access-token", auth_secret)
      .execute().returnContent().asString()
val obj = JSONObject.fromObject(script)  
val scriptContent = obj.getString("content")
val owner = obj.getString("owner")

```

最后提交给Console执行(Console会转发给)：

```scala
val res = Request.Post(PathFun(consoleUrl).add("/api_v1/run/script").toPath).
      connectTimeout(60 * 1000).socketTimeout(12 * 60 * 60 * 1000).
      addHeader("access-token", auth_secret).
      bodyForm(Form.form().add("sql", scriptContent).
        add("owner", owner).build(), Charset.forName("utf8"))
      .execute().returnResponse()
```


用户也可以将其转化为shell脚本或者封装成一个java类供第三方调度使用。