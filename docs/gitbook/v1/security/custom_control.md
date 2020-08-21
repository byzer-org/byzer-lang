#自定义接口访问策略

除了前面提到的Token验证，用户也可以开发自己的验证逻辑，比如请求验证服务器等。
可以在启动脚本里配置如下参数：

```
--jars your-jar-contains-auth-class
--conf spark.mlsql.auth.custom=your-auth-class-name
```

现阶段（2.0.0）版本，你需要用scala开发，保证验证auth-class里有如下函数：

```scala
def auth(params: Map[String, String]): (Boolean, String)
```

params会包含owner,sql等各种相关信息。 第一个返回值是布尔值，true表示通过验证，false表示没有通过验证。第二个返回值是当第一个返回值为false的情况下，给出的原因说明。