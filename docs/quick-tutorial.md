## 快速上手和体验


### step1: 下载资源

1. 到[release页面](http://download.mlsql.tech)下载最新jar包。
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
