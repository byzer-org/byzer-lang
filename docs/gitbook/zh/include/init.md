# 如何执行初始化脚本

在实际场景中，我们经常会需要启动系统时，执行一些特定的操作，比如设置connect连接信息等。
初始化动作我们一般会放在json配置文件中，启动时需要设置对应的json文件。

```sql
-streaming.job.file.path /tmp/init.json
```

下面是我的初始化脚本：

```json
{
  "init": {
    "desc": "初始化脚本",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "init",
        "params": [
          {
            "mlsql": [
              "select 'a' as a as table1;"
            ],
            "owner": "william",
            "home":"/tmp/william"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
简单说明下，`"name": "init",` 表示要执行初始化动作，mlsql则表示内部应该为mlsql脚本代码。 owner表示当前初始化时以什么用户执行，
home表示当前用户的主目录.

当系统启动时，会自动执行这一条MLSQL语句。

如果有上百条语句connect语句以及密码设置，都放在json文件里会很麻烦，因为json文件并不好书写。一个简单的方案是将这些信息放到console里，
比如我创建了一个Init/init.mlsql的脚本,其中Init是Console里的一个根目录。init.mlsql对应的内容如下：

```sql
select 'a' as a as table1;
```

现在我们在初始化json文件里，引入这个脚本：

```json
{
  "init": {
    "desc": "初始化脚本",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "init",
        "params": [
          {
            "mlsql": [
              "include Init.`init.mlsql`;"
            ],
            "owner": "william",
            "home":"/tmp/william"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
这样Engine 会去调用Console获取init.mlsql的脚本内容并且执行。

当然，还有一种方式是我们将数据init.mlsql脚本放到HDFS上，接着我们按如下方式引入：

```json
{
  "init": {
    "desc": "初始化脚本",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "init",
        "params": [
          {
            "mlsql": [
              "include hdfs.`/tmp/init.mlsql`;"
            ],
            "owner": "william",
            "home":"/tmp/william"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
值得注意的是,因为设置了主目录，include实际加载的目录会是 `/tmp/william/tmp/init.mlsql`。

关于include语法，大家可参看 文档[Include语法](http://docs.mlsql.tech/zh/grammar/include.html) 获得更多信息。