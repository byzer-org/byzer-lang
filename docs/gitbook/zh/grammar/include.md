# Include语法

和宏一样，include语法也是为了方便做代码的复用。

我们在脚本a.mlsql里定义了一个变量a：

```sql
set a= "a.mlsql"; 
```

接着在脚本b.mlsql里可以直接通过include进行引用：

```sql
include project.`a.mlsql`;

select "${a}" as col as output; 
```

通常，include语法需要配合MLSQL Console使用。因为默认MLSQL Console提供了脚本管理功能， MLSQL Engine会回调MLSQL Console从而获得
需要的脚本，然后进行include.

值得注意的是，include语法只是简单的将被包含的脚本展开到当前脚本中。

include 另外也支持hdfs,比如：

```sql
include hdfs.`/tmp/a.sql`;
```

或者http

```sql
include http.`http://abc.com` options 
param.a="coo" 
and param.b="wow";
and method="GET"
```

上面相当于如下方式请求获得脚本内容：

```sql
curl -XGET 'http://abc.com?a=coo&b=wow'
```


 