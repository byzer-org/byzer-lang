## MLSQL 模型部署

一旦我们通过MLSQL完成模型训练，这个时候，我们肯定想迫不及待的把模型部署然后提供API服务。
通常，模型使用的场景有三个：

1. 批处理。比如我们希望对一批数据做统一做一次预测处理。
2. 流式计算。 我们希望把模型部署在流式程序里。
3. API服务。 我们希望通过API 对外提供模型预测服务。
 
 
1，2 在MLSQL中是极好实现的。register完成后直接使用udf即可。如果用户想使用第三个方案，也是比较简单的。
首先，我们还是把StreamingPro 按标准的模式启动，记住，这个时候我们推荐使用local模式。一个典型的启动方式如下：

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name predict_service \
streamingpro-spark-2.0-1.0.0.jar    \
-streaming.name predict_service    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

接着我们通过 `://127.0.0.1:9003/run/script` 接口动态注册模型：

```sql
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
```

现在，我们可以通过`http://127.0.0.1:9003/model/predict` 接口进行预测了：

```sql

data=[[1,2,3...]]
sql=select bayes_predict(feature) as p
```

data 为一个json数组，数组里面的元素还是一个数组。 sql 则是允许用户使用前面注册的函数。其中feature字段名字是固定的。
不同的模型，可能函数的参数不同，具体需要查看MLSQL文档。

## 完整例子

启动一个训练的StreamingPro Service，提交如下脚本：

```sql
--NaiveBayes
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as NaiveBayes.`/tmp/bayes_model`;
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
select bayes_predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;

```

启动一个预测StreamingPro API Server,先注册模型：

```
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
```

接着就可以外部调用API使用了,需要传递两个参数：

```

data=[[1,2,3...]]
sql=select bayes_predict(feature) as p
```

最后的预测结果为：

```
{
    "p": {
        "type": 1,
        "values": [
            1,
            0
        ]
    }
}

```

当前版本data只支持多个向量，不支持张量。后续会支持传递shape。向量支持dense 和 sparse两种模式。如果是sparse 模式，则需要额外
传递参数vecSize

