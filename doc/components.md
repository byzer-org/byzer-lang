
StreamingPro is a fast,expresivive,and convenient cluster system running on Spark with streaming,batch,interactive,mllib support. 

It make devlopers more easy to build spark application without coding  by means of:

* Many powerfull modules which are easy to be reused
* SQL-Based processing 
* Script support
* Any data collection is treated  as table.

The  job  in StreamingPro is described by a json file . There are three main elements in it :

* Compositor  (Module)
* Strategy (How to combine configured moudles to work together)
* Ref (Register some extra libs can be resued by other jobs eg. UDF/meta-data Source)

```
{
  //jobname
  "esToCsv": {
    "desc": "job descriptions",
    "strategy": "streaming.core.strategy.SparkStreamingStrategy",//Strategy,default is linear combination
    "algorithm": [],// 
    "ref": [], // where you can  refrence  some common data source 
    "compositor": [
      {
        "name": "...source.SQLSourceCompositor",//data source
        "params": [
          {
            "format": "org.elasticsearch.spark.sql",//like jdbc driver  which can tell system how to communicate with storage.
            "path": "index/type",// path
            "es.nodes": "", //  paramters provied by specific storage . here is some paramters about elasticsearch
            "es.mapping.date.rich": "false",
            "es.scroll.size": "5000"
          }
        ]
      },
      {
        "name": "....transformation.JSONTableCompositor",
        "params": [
          {
            "tableName": "table1"  //module, register data source as a table named 'table1'
          }
        ]
      },
      {
        "name": "....transformation.SQLCompositor",
        "params": [
          {
            "sql": "select * from table1" // writting SQLs.
          }
        ]
      },
      {
        "name": "...output.SQLOutputCompositor",// Data persistence .
        "params": [
          {
            "format": "com.databricks.spark.csv",
            "path": "/tmp/csv-table1",
            "header": "true",
            "inferSchema": "true"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```


We also  divide  job flow into three  parts:

* Source .  The sources have two main types:
     
     * Streaming source . In streaming mode,  you should conver raw lines to json or map then  register them as  table.
     * Batch Source. In batch mode,  they are automatically converted to  table .
 
* Transformation. The lazy operation can be applied on the table,most important compositores are SQLCompositor and ScriptCompositor
* Output.  Data persistence 

### SQLSourceCompositor(Batch mode)

```
{
   "name": "streaming.core.compositor.spark.source.SQLSourceCompositor",
   "params": [{
                 "format":"org.apache.spark.sql.execution.datasources.kafka",
                 "path":"/tmp/offset/yyyyMMddHHmmss_jobname",
                  
                 "topics":"your topic",
                 "metadata.broker.list":"brokers"
             }]
}

```

format contains:

```
parquet
json
org.apache.spark.sql.execution.datasources.kafka
org.apache.spark.sql.execution.datasources.hdfs
org.elasticsearch.spark.sql
com.databricks.spark.csv

```

/tmp/offset/yyyyMMddHHmmss_jobname  content:

```
topic,partition ,offset
.....
topic,partition ,offset
```


### Kafka Compositor(Streaming Source Compositor)

```
{
   "name": "streaming.core.compositor.spark.source.SQLSourceCompositor",
   "params": [{
                 "topics":"your topic",
                 "metadata.broker.list":"brokers",
                 "auto.offset.reset": "smallest|largest"
             }]
}

```

### MockInputStreamCompositor(Streaming Source Compositor)

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

### MockInputStreamFromPathCompositor (Streaming Source Compositor)

```
{
        "name": "streaming.core.compositor.spark.streaming.source.MockInputStreamFromPathCompositor",
        "params": [{"path":"file:///tmp/test.txt"}]
}
```

Load test data from file.

### JDBC Compositor(Batch Source Compositor)

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
