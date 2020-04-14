# 开发权限客户端插件

【文档更新日志：2020-04-10】

> Note: 本文档适用于MLSQL Engine 1.3.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3

前面我们介绍过基本的表数据表示格式。  
现在我们介绍下用如何自定义开发一个client与你的权限系统进行交互。

## 表实现示例

```scala
package streaming.dsl.auth.client

import java.nio.charset.Charset

import org.apache.http.client.fluent.{Form, Request}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * 2019-03-13 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {

    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = context.owner
    val jsonTables = JSONTool.toJsonStr(tables)
    logDebug(format(jsonTables))
    val authUrl = context.userDefinedParam("__auth_server_url__")
    val auth_secret = context.userDefinedParam("__auth_secret__")
    try {

      val returnJson = Request.Post(authUrl).
        bodyForm(Form.form().add("tables", jsonTables).
          add("owner", owner).add("home", context.home).add("auth_secret", auth_secret)
          .build(), Charset.forName("utf8"))
        .execute().returnContent().asString()

      val res = JSONTool.parseJson[List[Boolean]](returnJson)
      val falseIndex = res.indexOf(false)
      if (falseIndex != -1) {
        val falseTable = tables(falseIndex)
        throw new RuntimeException(
          s"""
             |Error:
             |
             |db:    ${falseTable.db.getOrElse("")}
             |table: ${falseTable.table.getOrElse("")}
             |tableType: ${falseTable.tableType.name}
             |sourceType: ${falseTable.sourceType.getOrElse("")}
             |operateType: ${falseTable.operateType.toString}
             |
             |is not allowed to access.
           """.stripMargin)
      }
    } catch {
      case e: Exception if !e.isInstanceOf[RuntimeException] =>
        throw new RuntimeException("Auth control center maybe down casued by " + e.getMessage)
    }

    List()
  }
}

```

从上面的例子可以看出，用户需要实现TableAuth接口以及里面的auth方法，系统通过参数将前面我们描述的
`List[MLSQLTable]` 传递给你。接着你就可以将这些信息转化为用户已有【权限系统】可以识别的格式发给用户。
在默认示例里。最后返回`List[TableAuthResult]`。不过我这里简单的将异常抛出，并且告诉用户哪些是不可访问的。

在上面的例子，我将数据json格式化后，发给一个http接口，然后将结果解析在进行判断。最后，为了能够
让该类进行生效，用户启动的时候，需要配置：

```
--conf spark.mlsql.auth.implClass streaming.dsl.auth.client.DefaultConsoleClient
```

或者，在请求参数里 `context.__auth_client__` 带上全路径即可。




