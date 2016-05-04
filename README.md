# StreamingPro

This document is for StreamingPro developers. User's manual is now on its way.

## Introduction

StreamingPro is not a complete
application, but rather a code library and API that can easily be used
to build your streaming application which may run on Spark Streaming and Storm.

StreamingPro also make it possible that all you should do to build streaming program is assembling components(eg. SQL Component) in configuration file. 
Of source , if you are a geek who like do every thing by programing,we also encourage you use API provided
by StreamingPro which is more easy to use then original API designed by Spark/Storm.


## Setup Project

Since this project  depends on 

* [ServiceFramework](https://github.com/allwefantasy/ServiceFramework.git)
* [csdn_common](https://github.com/allwefantasy/csdn_common.git)
* [ServiceframeworkDispatcher](https://github.com/allwefantasy/ServiceframeworkDispatcher.git)

you should install them in your local maven repository/private maven repository to resolve the dependency issue.

Step 1

```
git clone https://github.com/allwefantasy/csdn_common.git
cd csdn_common
mvn -DskipTests clean install
```

step 2

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
mvn -DskipTests clean install
```

step 3

```
git clone https://github.com/allwefantasy/ServiceframeworkDispatcher.git
cd ServiceframeworkDispatcher
mvn -DskipTests clean install
```

step 4 

```
git clone https://github.com/allwefantasy/streamingpro.git

```

step 5

```
Import StreamingPro to your IDE.
```

Tips:

StreamingPro is a maven project and Intellj Idea is recommended cause it  have more powerful scala support which make
 your coding life more easy.
 
 
## Run Your First Application
 
First make sure you have selected `debug` profile in your IDE,then you can run:

```
streaming.core.LocalStreamingApp
```

when stared , you can see some message like follow:


```
+---+---+
|  a|  b|
+---+---+
|  3|  5|
+---+---+
```

Congratulations, everything is fine and you just run your first Spark Streaming Application.


You can find `strategy.v2.json` in `src/main/resource-debug` directory which describe what your streaming application have 
done .

Suppose your streaming data source is Kafka,and you need metadata from MySQL to process lines from Kafka.
Then you can do like follow:

1. create new job flow named `test`.
1. create new dataSource named `testJoinTable`
1. declare table `testJoinTable` in `test`.`ref`
1. configure  MockInputStreamCompositor to mock kafka source
1. configure  SingleColumnJSONCompositor to convert string to Json string with key named `a`
1. configure  JSONTableCompositor  to create sql table `test`
1. configure  multi SQLCompositor to process data , and you can use table `testJoinTable` in sql.
1. finally, configure SQLPrintOutputCompositor to print result.

here is the detail of  configuration:  

```
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
            "sql": "select t2.a,t2.b from test2 t2, testJoinTable t3 where t2.a = t3.a"
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

## How To Add New Compositor

In StreamingPro,every transformation can be implemented by Compositor. Suppose 
you wanna implements `map` function in Spark Streaming and convert a line into json 
string.

Create a class `SingleColumnJSONCompositor` which extends `BaseMapCompositor`

```
class SingleColumnJSONCompositor[T] extends BaseMapCompositor[T, String, String] with CompositorHelper {

  def name = {
    config("name", _configParams)
  }

  override def map: (String) => String = {
    require(name.isDefined, "please set column name by variable `name` in config file")
    val _name = name.get
    (line: String) => {
      val res = new JSONObject()
      res.put(_name, line)
      res.toString
    }
  }
}
```

override map method and do anything you want to the line putted by Streaming program.


or you want add `repartition` function, do like follow:

```
class RepartitionCompositor[T, S: ClassTag, U: ClassTag] extends Compositor[T] with CompositorHelper{

   protected var _configParams: util.List[util.Map[Any, Any]] = _

   val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

   override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
     this._configParams = configParams
   }

   def num = {
     config[Int]("num",_configParams)
   }

   override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
     val dstream = middleResult(0).asInstanceOf[DStream[S]]
     val _num = num.get
     val newDstream = dstream.repartition(_num)
     List(newDstream.asInstanceOf[T])
   }

 }
```







 