
Everything in StreamingPro is designed around compositors which describe the flow of streaming job and composed by strategy. Some powerful Compositor can have their own config file.  

### Kafka Compositor(Source Compositor)

```
{
   "name": "streaming.core.compositor.spark.streaming.source.KafkaStreamingCompositor",
   "params": [{
                 "topics":"your topic",
                 "metadata.broker.list":"brokers",
                 "auto.offset.reset": "smallest|largest"
             }]
}

```

### MockInputStreamCompositor(Source Compositor)

```
{
        "name": "streaming.core.compositor.spark.streaming.source.MockInputStreamCompositor",
        "params": [{
                      "batch-1":["1","2","3"],
                      "batch-2":["1","2","3"],
                      "batch-3":["1","2","3"],
                      "batch-4":["1","2","3"]
                  }]
}
```

You can provide any data in `params` ,and every batch will read one of them in order. This compositor is useful when you are testing.

### MockInputStreamFromPathCompositor (Source Compositor)

```
{
        "name": "streaming.core.compositor.spark.streaming.source.MockInputStreamFromPathCompositor",
        "params": [{"path":"file:///tmp/test.txt"}]
}
```

Load test data from file.

### JDBC Compositor(Source Compositor)

```
{
        "name": "streaming.core.compositor.spark.source.JDBCCompositor",
        "params": [{
                      "url": "jdbc:postgresql:observer",
                      "datable": "schema.tablename"
                  }]
}
```
This compositor responsible for reading data from database as external table . 

More properties:

| Property Name | Meaning | 
|:-----------|:------------|
| url | The JDBC URL to connect to| 
| dbtable | The JDBC table that should be read. Note that anything that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could also use a subquery in parentheses.| 
|driver|The class name of the JDBC driver to use to connect to this URL.|
|partitionColumn, lowerBound, upperBound, numPartitions|These options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.|
|fetchSize|The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).|


### SingleColumnJSONCompositor(transformation)

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.SingleColumnJSONCompositor",
        "params": [{
            "name": "a"
          }]
}
```

This compositor responsible for transforming log line to JSon format with specified column name.

Suppose you have one line like `i am streaming pro`, then this compositor will wrap this line to JSon string '{"a":"i am streaming pro"}'. Of course ,you can define the column name.


### JSONTableCompositor(transformation)

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.JSONTableCompositor",
        "params": [{
            "tableName": "test"
          }]
}
```

Map JSon to Table named "tableName" which defined in params.


### ScalaMapToJSONCompositor

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.ScalaMapToJSONCompositor",
        "params": [{}]
}
```

Convert Scala Map to json String

### JavaMapToJSONCompositor 

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.JavaMapToJSONCompositor",
        "params": [{}]
}
```

Convert Java Map to json String



### FlatJSONCompositor

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.FlatJSONCompositor",
        "params": [{"a":"$['store']['book'][0]['title']"}]
}
```

Extracting value from json by XPATH,and named a new name . XPATH grammar: [JsonPath](https://github.com/jayway/JsonPath)


### NginxParserCompositor 

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.NginxParserCompositor",
        "params": [{"time":0,"url":1}]
}
```



### SQLCompositor(transformation)

```
{
        "name": "streaming.core.compositor.spark.streaming.transformation.SQLCompositor",
        "params": [
          {
            "sql": "select a, \"5\" as b from test",
            "outputTableName": "test2"
          }
        ]
      }
```

### SQLPrintOutputCompositor(output)

```
{
        "name": "streaming.core.compositor.spark.streaming.output.SQLPrintOutputCompositor",
        "params": [{}]
}
```

print output


### SQLESOutputCompositor(output)

```
{
        "name":"streaming.core.compositor.spark.streaming.output.SQLESOutputCompositor",
        "params":[
          {
            "es.nodes":"",
            "es.resource":"",
            "es.mapping.include":"",
            "timeFormat":"yyyyMMdd"
          }
        ]
}
```

if timeFormat configured,then the index name will be es.resource_yyyyMMdd


### JavaMapToJSONCompositor

```
 {
        "name": "streaming.core.compositor.spark.streaming.transformation.JavaMapToJSONCompositor",
        "params": [
          {
          }
        ]
      },
```
convert java map to JSON. So we can use `JSONTableCompositor` to register table.

### SparkStreamingStrategy (strategy)

```
"strategy": "streaming.core.strategy.SparkStreamingStrategy",
```

Use this strategy  to build a job flow.



### SparkStreamingStrategy (strategy)

```
"strategy": "streaming.core.strategy.SparkStreamingRefStrategy",
```

Use this strategy  to build a join table.

for example:

```
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
```

Use this snippet, you have create a table named "testJoinTable". Now you can use it in other job flow with ref property configured.

```
"ref": [
      "testJoinTable"
    ],
```
