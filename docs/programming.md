## 基于StreamingPro编程

### UDF 函数编写和配置
通过添加UDF函数，可以很好的扩充SQL的功能。
具体做法是，在配置文件添加一个配置，

```
"udf_register": {
    "desc": "测试",
    "strategy": "refFunction",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "sql.udf",
        "params": [
          {
            "analysis": "streaming.core.compositor.spark.udf.func.Functions"
          }
        ]
      }
    ]
  }
```

udf_register, analysis等都可以自定义命名，最好是取个有意义的名字，方便管理。

`streaming.core.compositor.spark.udf.func.Functions `包含了你开发的UDF函数。比如我要开发一个mkString udf函数：

```
object Functions {
  def mkString(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("mkString", (sep: String, co: mutable.WrappedArray[String]) => {
      co.mkString(sep)
    })
  }
}
```

之后就可以在你的Job的ref标签上引用了

```
{
  "your-first-batch-job": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": ['udf_register'],
```

`your-first-batch-job` 下所有的batch.sql 就可以使用这个自定义的`mkString` 函数了。

### Scala脚本编写和配置

另外，StreamingPro也支持script脚本（目前只支持scala脚本），因为在配置文件中，如果能嵌入一些脚本，在特定场景里也是很方便的，
这样既不需要编译啥的了。截止到这篇发布为止,支持脚本的有：

Spark 1.6.+:

    * 批处理

Spark 2.+:

     * 批处理
     * Spark Streaming处理

具体做法是使用batch.script.df 算子：

```
{
        "name": "batch.script.df",
        "params": [
          {
            "script": "context.sql(\"select a as t from test\").registerTempTable(\"finalOutputTable\")",
            "source": "-"
          }
        ]
      }
```

给出一个比较完整的例子：

```
{
  "batch-console": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "batch.sources",
        "params": [
          {
            "path": "file:///tmp/hdfsfile/abc.txt",
            "format": "json",
            "outputTable": "test"

          }
        ]
      },
      {
        "name": "batch.script.df",
        "params": [
          {
            "script": "context.sql(\"select a as t from test\").registerTempTable(\"finalOutputTable\")",
            "source": "-"
          }
        ]
      },
      {
        "name": "batch.outputs",
        "params": [
          {
            "name":"jack",
            "format": "console",
            "path": "-",
            "inputTableName": "finalOutputTable",
            "mode":"Overwrite"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

在json中写代码是一件很复杂的事情，你也可以把代码放在另外一个文件中，然后引用该文件即可，具体做法如下，

```
{
        "name": "batch.script.df",
        "params": [
          {
            "script": "file:///tmp/raw_process.scala",
            "source": "file"
          }
        ]
},
```

前面的案例是暴露了sqlContext给你，显得有点太灵活，而且这个方案因为使用了动态编译，有部分场景会有异常。所以一个更好的办法是
依然使用表作为交互的方式，具体使用如下：


```
{
        "name": "batch.script",
        "params": [
          {
            "inputTableName": "test",
            "outputTableName": "test3",
            "schema": "file:///tmp/raw_schema.scala",
            "useDocMap":true
          },
          {
            "raw": "file:///tmp/raw_process.scala"
          }
        ]
      },
```

其中raw 是一段scala代码。我们定义了inputTableName作为输入，那么这段代码就是处理这个表的，你需要给出输出，以及对应的
输出的schema类型。

/tmp/raw_process.scala 的代码如下：

```
val Array(a,b)=doc("raw").toString.split("\t")
           Map("a"->a,"b"->b)

```

doc其实就是inputTableName,这是一个Map[String,Any]结构的数据。

/tmp/raw_schema.scala" 的代码如下：

```
Some(StructType(Array(StructField("a", StringType, true),StructField("b", StringType, true))))
```


## streamingpro api 高级编程
StreamingPro也提供了API,可以定制任何你要的环节，并且和其他现有的组件可以很好的协同，当然，你也可以使用原始的Compositor接口，
实现 非常高级的功能。目前支持的版本和类型有：
Spark 2.+:

     * 批处理
     * Spark Streaming处理


这里有个spark streaming的例子，我想先对数据写代码处理，然后再接SQL组件，然后再进行存储（存储我也可能想写代码）
```
{
  "scalamaptojson": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [
    ],
    "compositor": [
      {
        "name": "stream.sources",
        "params": [
          {
            "format": "socket",
            "outputTable": "test",
            "port": "9999",
            "host": "localhost",
            "path": "-"
          }
        ]
      },
      {
        "name": "stream.script.df",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestTransform",
            "source": "-"
          }
        ]
      },
      {
        "name": "stream.sql",
        "params": [
          {
            "sql": "select * from test2",
            "outputTableName": "test3"
          }
        ]
      },
      {
        "name": "stream.outputs",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestOutputWriter",
            "inputTableName": "test3"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

要实现上面的逻辑，首先是创建一个项目，然后引入下面的依赖：

```
  <dependency>
            <groupId>tech.mlsql</groupId>
            <artifactId>streamingpro-api</artifactId>
            <version>2.0.0</version>
        </dependency>
```
这个包目前很简单，只有两个接口：

```
//数据处理
trait Transform {
  def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit
}

//数据写入
trait OutputWriter {
  def write(df: DataFrame, contextParams: Map[Any, Any], config: Map[String, String]): Unit
}
```

以数据处理为例，只要实现Transform接口，就可以通过stream.script.df 模块进行配置了。
```
 {
        "name": "stream.script.df",
        "params": [
          {
            "clzz": "streaming.core.compositor.spark.api.example.TestTransform",
            "source": "-"
          }
        ]
      },
```

同样，我们也对输出进行了编程处理。
下面是TestTransform的实现：

```
class TestTransform extends Transform {
  override def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sql("select * from test").createOrReplaceTempView("test2")
  }
}
```

TestOutputWriter也是类似的：

```
class TestOutputWriter extends OutputWriter {
  override def write(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sparkSession.table(config("inputTableName")).show(100)
  }
}
```

contextParams 是整个链路传递的参数，大家可以忽略。config则是配置参数，比如如上面配置中的source参数，clzz参数等。另外这些参数都是可以通过启动脚本配置和替换的，参看[如何在命令行中指定StreamingPro的写入路径](http://www.jianshu.com/p/edaa0c124933)
