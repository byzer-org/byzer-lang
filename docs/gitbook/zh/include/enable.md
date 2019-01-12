# 如何开启include功能

inlcude脚本功能默认是开启的，但是MLSQL-Engine本身并不提供脚本的存储，所以在include会遇到阻碍，需要有专门的服务提供
获取MLSQL脚本的接口。交互流程大致如下：


```
  MLSQL-Console   -------> MLSQL-Engine
                                       |
                                       |  解析include,然后回调Console
  MLSQL-Console API  < -----------------               
```

下面是MLSQL-Console提供的脚本接口实现：

```scala

@At(path = Array("/api_v1/script_file/include"), types = Array(Method.GET))
  def includeScriptFile = {
    user = MlsqlUser.findByName(param("owner"))
    val path = param("path")
    val node = scriptFileService.findScriptFileByPath(user, path)
    render(200, node.getContent())
  }
```


对于如下的语句：

```sql
include your-project-name.`a.b.c`;
```

MLSQL解析后，会把当前用户名 owner,以及传递path给你。path的值如下：

```
your-project-name.a.b.c
```  

MLSQL-Console需要根据这个路径解析去拿到 c.mlsql脚本的内容，然后返回。

那么MLSQL-Engine 如何知道MLSQL-Console的URL地址呢？

在请求MLSQL-Engine的地址时，请配置如下参数：

```
http://127.0.0.1:9003/run/script

参数：

context.__default__include_fetch_url__=http://127.0.0.1:9002

```

这样MLSQL-Engine就知道该到哪去获取脚本内容了。

## 总结

为了开启include脚本功能，

1. 需要你在请求MLSQL-Engine时，通过参数`context.__default__include_fetch_url__`告知脚本服务地址。
2. 脚本存储服务需要自己开发，大家可以可以参考MLSQL-Console的实现。

MLSQL-Engine会回调脚本存储服务，将include的路径以及用户传递给脚本存储服务。 