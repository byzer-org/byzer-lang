## 快速上手和体验


### step1: 下载资源

你即可以通过自己手动编译一个jar包出来，也可以到[release页面][https://github.com/allwefantasy/streamingpro/releases]下载最新jar包。
一旦获取jar包后，你还需要下载一个[spark发行版](https://www.apache.org/dyn/closer.lua/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz)。

### step2 解压进入spark home目录，启动服务

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
streamingpro-spark-2.0-1.0.0.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

query.json是一个包含"{}" 字符串的文件，[更多参数](https://github.com/allwefantasy/streamingpro#streamingpro的一些参数)解释。

大家看到，这是一个标准的Spark程序启动命令。然后以单'-streaming'开头的则是StreamingPro特有的参数。一旦启动后，就可以通过http 9003端口
进行交互了。你可以提交一些SQL脚本即可完成包括爬虫、ETL、流式计算、算法、模型预测等功能。

### step3:

通过postman 等http交互工具，访问 `http://127.0.0.1:9003/run/script` 参数名称为sql，具体脚本如下：

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as NaiveBayes.`/tmp/bayes_model`;
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
select bayes_predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```

其中里面的 `/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` 是Spark安装包里已经有的文件。

完成之后你可以打开 /tmp/result 目录查看结果。

你也可以通过 `http://127.0.0.1:9003/run/sql=...` 传递 `select * from result` 参数，然后就能看到前面的预测结果，而不用打开文件。