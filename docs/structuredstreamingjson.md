## 执行StructuredStreaming任务

原生Structured Streaming不支持Kafka 0.8,0.9,所以StreamingPro则对此提供了支持，对应的format名称是 kafka8/kafka9。
和Spark Streaming一样，一个Job里只能包含一个Kafka源。一个简单示例如下：



```
{
  "your-fist-ss-job": {
    "desc": "测试",
    "strategy": "spark",
    "algorithm": [],
    "ref": [
    ],
    "compositor": [
      {
        "name": "ss.sources",
        "params": [
          {
            "format": "kafka9",
            "outputTable": "test",
            "kafka.bootstrap.servers": "127.0.0.1:9092",
            "topics": "test",
            "path": "-"
          },
          {
            "format": "com.databricks.spark.csv",
            "outputTable": "sample",
            "header": "true",
            "path": "/Users/allwefantasy/streamingpro/sample.csv"
          }
        ]
      },
      {
        "name": "ss.sql",
        "params": [
          {
            "sql": "select city as value from test left join sample on  CAST(test.value AS String) == sample.name",
            "outputTableName": "test3"
          }
        ]
      },
      {
        "name": "ss.outputs",
        "params": [
          {
            "mode": "append",
            "format": "kafka8",
            "metadata.broker.list":"127.0.0.1:9092",
            "topics":"test2",
            "inputTableName": "test3",
            "checkpoint":"/tmp/ss-kafka/",
            "path": "/tmp/ss-kafka-data"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

这个例子从Kafka读取，经过处理后写入Kafka的另外一个topic