Run Your First Application

First make sure you have selected `debug` profile in your IDE,then you can run:

streaming.core.LocalStreamingApp
when started , you can see some message like follow:

```
+---+---+
|  a|  b|
+---+---+
|  3|  5|
+---+---+
```

Congratulations, everything is fine and you just run your first Spark Streaming Application.

You can find `strategy.v2.json` in `src/main/resource-debug` directory which describe what your streaming application have done .

Suppose your streaming data source is Kafka,and you need metadata from MySQL to process lines from Kafka. Then you can do like follow:

create new job flow named `test`.
create new dataSource named `testJoinTable`
declare table `testJoinTable` in `test.ref`
configure MockInputStreamCompositor to mock kafka source
configure SingleColumnJSONCompositor to convert string to Json string with key named `a`
configure JSONTableCompositor to create sql table `test`
configure multi SQLCompositor to process data , and you can use table `testJoinTable` in sql.
finally, configure SQLPrintOutputCompositor to print result.
here is the detail of configuration:

```json?linenums
{
  "test": {
    "desc": "测试",
    "strategy": "streaming.core.strategy.SparkStreamingStrategy",
    "algorithm": [],
    "ref": [
      "testJoinTable"
    ],
    "compositor": [
      {
        "name": "streaming.core.compositor.spark.streaming.source.MockInputStreamCompositor",
        "params": [{"data1":["1","2","3"]}]
      },
      {
        "name": "streaming.core.compositor.spark.streaming.transformation.SingleColumnJSONCompositor",
        "params": [
          {
            "name": "a"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.streaming.transformation.JSONTableCompositor",
        "params": [
          {
            "tableName": "test"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.streaming.transformation.SQLCompositor",
        "params": [
          {
            "sql": "select a, \"5\" as b from test",
            "outputTableName": "test2"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.streaming.transformation.SQLCompositor",
        "params": [
          {
            "sql": "select t2.a,t2.b from test2 t2, test t3 where t2.a = t3.a"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.streaming.output.SQLPrintOutputCompositor",
        "params": [
          {
          }
        ]
      }
    ],
    "configParams": {
    }
  },
  "testJoinTable": {
    "desc": "测试",
    "strategy": "streaming.core.strategy.SparkStreamingRefStrategy",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "streaming.core.compositor.spark.source.MockJsonCompositor",
        "params": [
          {"a":"3"},
          {"a":"4"},
          {"a":"5"}
        ]
      },
      {
        "name": "streaming.core.compositor.spark.transformation.JSONTableCompositor",
        "params": [
          {
            "tableName": "testJoinTable"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
