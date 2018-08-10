# Documentation in English

## What's StreamingPro and MLSQL?

StreamingPro is mainly designed to run on Apache Spark but it also supports Apache Flink for the runtime.
Thus, it can be considered as a cross,distributed platform which is the combination of BigData platform and AI platform 
You can run  both Big Data Processing and Machine Learning script in StreamingPro.


MLSQL is a DSL akin to SQL but more powerfull based on StreamingPro platform. Since StreamingPro have already 
intergrated many ML framework including Spark MLLib, DL4J and Python ML framework eg. Sklearn, Tensorflow(supporting cluster mode)
this means you can use MLSQL to operate all these popular Machine Learning frameworks.

## Why MLSQL

MLSQL give you the power to use just one SQL-like language to finish all your Machine Learning  pipeline.  It also provide so 
many modules and functions to help you simplify the complex of build Machine Learning application.
  
1. MLSQL is the only one language you should take over.
2. Data preproccessing created in training phase can also be used in streaming, batch , API service directly without coding.
3. Server mode make you get rid of environment trouble.      


## Quick Tutorial

Step 1:


Download the jars from the release page: [Release页面](https://github.com/allwefantasy/streamingpro/releases):

1. streamingpro-mlsql-1.1.2.jar
2. ansj_seg-5.1.6.jar
3. nlp-lang-1.7.8.jar

Step 2:

Visit the downloads page: [Spark](https://spark.apache.org/downloads.html), to download Apache Spark 2.2.0 and then unarvhive it.
 
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


## Documentation in Chinese

### step1: 下载资源

1. 到[release页面](https://github.com/allwefantasy/streamingpro/releases)下载最新jar包。
2. 下载一个[spark发行版](https://www.apache.org/dyn/closer.lua/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz)。

### step2 解压进入spark home目录，启动服务

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
streamingpro-spark-2.0-1.1.0.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

大家复制黏贴就好，其中

1. query.json是一个包含"{}" 字符串的文件
2. Streamingpro-spark-2.0-1.1.0.jar 是你刚才下载的StreamingPro jar包。
3. 其他参数大家照着写就好

如果对众多参数感兴趣，不妨移步：[更多参数](https://github.com/allwefantasy/streamingpro#streamingpro的一些参数)

值得注意，这是一个标准的Spark程序启动命令，其中以"--"开头的是spark自有参数，而以'-streaming'开头的则是StreamingPro特有的参数，这里需要区别一下。
一旦启动后，就可以通过9003端口进行交互了。你可以提交一些SQL脚本即可完成包括爬虫、ETL、流式计算、算法、模型预测等功能。我们推荐使用Postman这个
HTTP交互软件来完成交互。

### step3: 玩一个ETL处理

目标是随便造一些数据，然后保存到mysql里去：

```sql
select "a" as a,"b" as b
as abc;

-- 这里只是表名你是可以使用上面的表形成一张新表
select * from abc
as newabc;

save overwrite newabc
as jdbc.`tableau.abc`
options truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
```

当然如果你能连接hive,写hive数据库以及表名就可以了。这里你可以实现非常复杂的逻辑，并且支持类似set语法[PR-181](https://github.com/allwefantasy/streamingpro/pull/181)


### step3.1: 玩一把算法

执行脚本的接口是 `http://127.0.0.1:9003/run/script` ，接受的主要参数是sql。 下面是一段sql脚本， 

```sql

-- 加载spark项目里的一个测试数据
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

-- 训练一个贝叶斯模型，并且保存在/tmp/bayes_model目录下。
train data as NaiveBayes.`/tmp/bayes_model`;

-- 注册训练好的模型
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;

-- 对所有数据进行预测
select bayes_predict(features)  from data as result;

-- 把预测结果保存到/tmp/result目录下，格式为json。
save overwrite result as json.`/tmp/result`;
```

该接口执行成功会返回"{}" ，如果失败，会有对应错误。
其中 `/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` 是Spark安装包里已经有的文件。

完成之后你可以打开 /tmp/result 目录查看结果，当然你可以可以通过`http://127.0.0.1:9003/run/sql=select * from result` 
查看预测结果。

### 继续玩

玩一把流式计算，参看[流式计算](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-stream.md)


## 应用模式和服务模式

1. 应用模式：写json配置文件，StreamingPro启动后执行该文件，可以作为批处理或者流式程序。
2. 服务模式：启动一个StreamingPro Server作为常驻程序,然后通过http接口发送MLSQL脚本进行交互。

我们强烈推荐使用第二种模式，第一种模式现在已经不太更新了，现在迅速迭代的是第二种模式，并且第二种模式可以构建AI平台。
为了避免编译的麻烦，你可以直接使用[release版本](https://github.com/allwefantasy/streamingpro/releases)

对于确实需要使用json配置文件的，我们也提供了batch.mlsql脚本，可以让你使用mlsql语法，例如（v1.1.2开始具有这个功能）：

```json
{
  "mlsql": {
    "desc": "测试",
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

## 编译

* [编译文档](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md)
* [编译DSL文档](./docs/generate-dsl-java-source.md)

## 高级编程
* [自定义基于Python的算法模型](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-user-defined-alg.md)

## 使用MLSQL做机器学习
* [MLSQL使用SKLearn做文本分类实例](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-nlp-example.md)
* [MLSQL-分布式算法 based on spark MMLib](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-mmlib.md)
* [MLSQL-深度学习 based on TensorFlow](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-tensorflow.md)
* [MLSQL-单机算法 based on SKLearn](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-sklearn.md)
* [MLSQL日志回显](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-log-monitor.md)


## 部署模型API服务
* [如何把模型部署成API服务](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-model-deploy.md)


## MLSQL常用功能

* [MLSQL语法说明](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-grammar.md)

* [各数据源读取和写入](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-datasources.md)
* [如何使用CarbonData作为存储](https://github.com/allwefantasy/streamingpro/blob/master/docs/carbondata.md)

* [常见特征处理模型说明](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-data-processing-model.md)
* [常见向量操作Vector操作函数](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-functions.md)
* [如何在MLSQL中运用分词抽词工具](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)

* [网页抓取](https://github.com/allwefantasy/streamingpro/blob/master/docs/crawler.md)
* [批处理计算](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-batch.md)
* [流式计算](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-stream.md)



## 使用配置完成Spark编程
1. [Spark 批处理](https://github.com/allwefantasy/streamingpro/blob/master/docs/batchjson.md)
1. [Spark Streaming](https://github.com/allwefantasy/streamingpro/blob/master/docs/sparkstreamingjson.md)
1. [Structured Streaming](https://github.com/allwefantasy/streamingpro/blob/master/docs/structuredstreamingjson.md)
1. [StreamingPro对机器学习的支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/mljson.md)

## 概览：

1. [概述](https://github.com/allwefantasy/streamingpro/blob/master/README.md#概述)
1. [项目模块说明](https://github.com/allwefantasy/streamingpro/blob/master/README.md#项目模块说明)
1. [编译](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md)
1. [相关概念](https://github.com/allwefantasy/streamingpro/blob/master/README.md#相关概念)
1. [StreamingPro的一些参数](https://github.com/allwefantasy/streamingpro/blob/master/README.md#StreamingPro的一些参数)

## 周边工具
1. [StreamingPro Manager](https://github.com/allwefantasy/streamingpro/blob/master/docs/manager.md)
1. [StreamingPro json文件编辑器支持](https://github.com/allwefantasy/streamingpro/blob/master/README.md#StreamingPro-json文件编辑器支持)

## 实验
1. [flink支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/flink.md)

## 其他文档

* [英文文档](https://github.com/allwefantasy/streamingpro/blob/master/README-EN.md)
* [简书专栏](https://www.jianshu.com/c/759bc22b9e15)
* [源码阅读-深入浅出StreamingPro](https://github.com/allwefantasy/streamingpro/issues/47)
* [为什么开发MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/why-develop-mlsql.md)
* [SQL服务](https://github.com/allwefantasy/streamingpro/blob/master/docs/sqlservice.md)



















