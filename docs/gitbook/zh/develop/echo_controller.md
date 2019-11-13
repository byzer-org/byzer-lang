# App 插件开发指南

MLSQL提供了 [插件商店](https://docs.mlsql.tech/zh/plugins/),方便开发者发布自己开发的插件。

MLSQL 支持四种类型的插件：

1. ET 插件
2. DataSource 插件
3. Script 插件
4. App 插件

其中App 插件主要解决两个问题：

1. MLSQL Engine执行SQL的入口是/run/script 接口。App可以拦截该接口，从而实现自定义执行逻辑。
2. 启动时执行特定的代码

对于第二个问题，典型的场景比如，流式程序的信息是需要持久化的，如果MLSQL Engine挂掉了，再次启动的时候，应该能够将原有的流程序拉起。
这个时候我们就可以提供一个相关的App插件来完成这些工作。譬如 App插件[stream-boostrap-at-startup](https://github.com/allwefantasy/mlsql-pluins/tree/master/stream-boostrap-at-startup)就完成了类似的功能。

对于第一个，我们知道，我们没办法控制整个脚本，因为其他三个插件都是优化其中的某一个环节。如果我需要对整个脚本进行预处理，这个时候就需要
App插件来支持。

下面，我们会简要的开发一个Echo插件，用户传递什么样的SQL过来，我们返回什么样的SQL回去。新建一个类：

```scala
package tech.mlsql.plugins.app.echocontroller

import tech.mlsql.app.CustomController
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.version.VersionCompatibility

/**
 * 7/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class StreamApp extends tech.mlsql.app.App with VersionCompatibility {
  override def run(args: Seq[String]): Unit = {
    AppRuntimeStore.store.registerController("echo", classOf[EchoController].getName)
  }

  override def supportedVersions: Seq[String] = Seq("1.5.0-SNAPSHOT", "1.5.0")
}
```
实现 App/VersionCompatibility 接口。

接着实现具体的逻辑：

```scala
class EchoController extends CustomController {
  override def run(params: Map[String, String]): String = {
    JSONTool.toJsonStr(List(params("sql")))
  }
}
```

params是一个Map,包含了所有http请求传递过来的参数。最后返回的必须是一个json字符串。用户可以通过params拿到owner,sql等各种信息，
然后决定采取何种策略进行处理。

最后，打包好插件之后，通过如下命令安装：

```
!plugin app add tech.mlsql.plugins.app.echocontroller.StreamApp echo;
```
