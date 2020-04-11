#  ET组件的权限

【文档更新日志：2020-04-10】

> Note: 本文档适用于MLSQL Engine 1.3.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3

MLSQL支持四种插件形式，其中ET是最常见的一种。他可以极大的扩展MLSQL的功能。

```
!plugin et add - 'last-command';
```

这个!plugin命令其实自身就是一个ET插件。我们完全有可能希望这个命令只能特定的用户才能使用。
这个时候，我们需要对ET进行权限控制。

还记得我们之前说，所有的权限都可以抽象成表。对于任何ET，如果需要进行权限控制，只需要实现ETAuth接口即可。
下面是plugin的实现：

```scala
class PluginCommand(override val uid: String) extends SQLAlg with ETAuth with WowParams {
 override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__plugin_operator__"),
      OperateType.INSERT,
      Option("_mlsql_"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }
  }
}

```

核心是实现了auth方法，该方法，我们将!plugin插件定义为系统表，表示某用户对系统表 `__plugin_operator__`进行insert操作。
如果权限中心允许当前用户执行该操作，那么会通过，否则会根据client实现得到不同的效果。
