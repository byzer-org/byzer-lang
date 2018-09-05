## What's StreamingPro and MLSQL?

StreamingPro is mainly designed to run on Apache Spark but it also supports Apache Flink for the runtime.
Thus, it can be considered as a cross,distributed platform which is the combination of BigData platform and AI platform 
where you can run  both Big Data Processing and Machine Learning script.


MLSQL is a DSL akin to SQL but more powerfull based on StreamingPro platform. Since StreamingPro have already 
intergrated many ML frameworks including Spark MLLib, DL4J and Python ML framework eg. Sklearn, Tensorflow(supporting cluster mode)
this means you can use MLSQL to operate all these popular Machine Learning frameworks.

## Why MLSQL

MLSQL give you the power to use just one SQL-like language to finish all your Machine Learning  pipeline.  It also provides so 
many modules and functions to help you simplify the complexity of building Machine Learning application.
  
1. MLSQL is the only one language you should take over.
2. Data preproccessing created in training phase can also be used in streaming, batch , API service directly without coding.
3. Server mode make you get rid of environment trouble.      


## Quick Tutorial

Step 1:


Download the jars from the release page: [Release页面](https://github.com/allwefantasy/streamingpro/releases):

1. streamingpro-mlsql-1.x.x.jar
2. ansj_seg-5.1.6.jar
3. nlp-lang-1.7.8.jar

Step 2:

Visit the downloads page: [Spark](https://spark.apache.org/downloads.html), to download Apache Spark 2.2.0 and then unarvhive it.

2.1 unarvhive the Apache Spark 2.2.0 package  
    tar -zxvf spark-2.2.0-bin-hadoop2.7.tgz
      

2.2 configure enviroment variable  
    vi /etc/profile  
    :i add the following sentence at the end of the file

    export SPARK_HOME=/your/path/spark-2.2.0-bin-hadoop2.7  
    export PATH=$PATH:$SPARK_HOME/bin  
    
    :wq save the file and quit  

2.3 load the new configuration  
    source /etc/profile  

Step 3:

```shell
cd spark-2.2.0-bin-hadoop2.7/

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[*] \
--name sql-interactive \
--jars ansj_seg-5.1.6.jar,nlp-lang-1.7.8.jar
streamingpro-mlsql-1.1.2.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

`query.json` is a json file contains "{}".

Step 4: 

Open your chrome browser, type the following url:

```
http://127.0.0.1:9003
```

![](https://github.com/allwefantasy/mlsql-web/raw/master/images/WX20180629-105204@2x.png)

Enjoy.


---------------------------------------------------
Run the first Machine Learning Script in MLSQL.

```sql

-- load data from spark distribution 
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

-- train a NaiveBayes model and save it in /tmp/bayes_model.
-- Here the alg we use  is based on Spark MLlib 
train data as NaiveBayes.`/tmp/bayes_model`;

-- register your model
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;

-- predict all data 
select bayes_predict(features) as predict_label, label  from data as result;

-- save predicted result in /tmp/result with json format
save overwrite result as json.`/tmp/result`;

-- show predict label in web table.
select * from result as res;
```

Please make sure the path `/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` is correct.

Copy and paste the script to the web page, and click `运行`, then you will see the label and predict_label.

Congratulations, you have completed the first Machine Learning script!

----------------------------------------------------

Run the first ETL Script In MLSQL.


```sql
select "a" as a,"b" as b
as abc;

-- here we just copy all from table abc and then create a new table newabc.

From Oscar:
-- we just copy all from table abc and create a new table newabc here.

select * from abc
as newabc;

-- save the newabc table to mysql.
save overwrite newabc
as jdbc.`db.abc`
options truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
```

Congratulations, you have completed the first ETL script!

-------------------------------------------------------


## Run as Application or Server

1. Application mode： Run StreamingPro as a application which executes a json file.
2. Server mode：Run StreamingPro as a server and you can interactive with it with http protocol. 

We strongly recommend users to deploy StreamingPro with Server mode. Server mode is developed actively.

In order to avoid compiling problems, please use [release version](https://github.com/allwefantasy/streamingpro/releases)
directly.

If you really want to use application mode, StreamingPro supports `batch.mlsql` keyword in json file, 
so you can still use mlsql grammar.(This function provided from v1.1.2)


```json
{
  "mlsql": {
    "desc": "test",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "batch.mlsql",
        "params": [
          {
            "sql": [
              "select 'a' as a as table1;",
              "save overwrite table1 as parquet.`/tmp/kk`;"
            ]
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

## Learning MLSQL

* [MLSQL Grammar](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-grammar.md)
* [Using Build-in Algorithms](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-build-in-algorithms.md)
* [Scala/Python UDF](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-script-support.md)
* [Stream Jobs](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-stream.md)
* [Using Python ML Framework To Train And Predict Within MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-python-machine-learning.md)

## Compiling

* [How to compile](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/compile.md)
* [How to compile DSL module](./docs/generate-dsl-java-source.md)

## Advanced Programming
* [How to implements user defined algorithm in MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-user-defined-alg.md)

## Machine Learning

* [How to use spark MMLib in MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-mmlib.md)
* [How to use TensorFlow in MLSQL ](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-tensorflow.md)
* [How to use SKlearn in MLSQL ](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-sklearn.md)
* [SKlearn example](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-nlp-example.md)
* [How to reiceive logs from StreamingPro](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-log-monitor.md)


## Model deploy
* [How to deploy your predict service](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-model-deploy.md)


## MLSQL

* [Datasources](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-datasources.md)
* [How to use CarbonData as storage](https://github.com/allwefantasy/streamingpro/blob/master/docs/carbondata.md)

* [Preprecessing modules in train statement](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-data-processing-model.md)
* [Preprecessing functions in select statement](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-functions.md)
* [How to use text analyzer in MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)

* [How to use MLSQL to crawl the web pages](https://github.com/allwefantasy/streamingpro/blob/master/docs/crawler.md)
* [How to use MLSQL to do batch processing](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-batch.md)
* [How to use MLSQL to do streaming processing](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-stream.md)



## Tools
1. [StreamingPro Manager](https://github.com/allwefantasy/streamingpro/blob/master/docs/manager.md)
1. [StreamingPro json editor](https://github.com/allwefantasy/streamingpro/blob/master/README.md#StreamingPro-json文件编辑器支持)

## experiment
1. [flink support](https://github.com/allwefantasy/streamingpro/blob/master/docs/flink.md)

## Other documents

* [简书专栏](https://www.jianshu.com/c/759bc22b9e15)
* [源码阅读-深入浅出StreamingPro](https://github.com/allwefantasy/streamingpro/issues/47)
* [为什么开发MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/why-develop-mlsql.md)
* [SQL服务](https://github.com/allwefantasy/streamingpro/blob/master/docs/sqlservice.md)



















