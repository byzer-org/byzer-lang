### 日志回显
MLSQL是基于Spark Service,当不同租户同时使用时，日志会混在一起，并且分布在driver和executor端。
当是在训练模型时，我们需要看到模型的训练进度。
为此，MLSQL针对SKlearn/Tensorflow初步提供了一套回显机制,会将不同租户的信息写入到Kafka中。

具体用法如下，当用户在进行train的时候，可以设置`kafkaParam.userName`参数：

```sql
train newdata as TensorFlow.`/tmp/model2`
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and   `kafkaParam.topic`="test"
and   `kafkaParam.group_id`="g_test-1"
and   `kafkaParam.reuse`="false"
-- 设置一个userName参数
and   `kafkaParam.userName`="William" 
and   `fitParam.0.layerGroup`="300,100"
and   `fitParam.0.epochs`="1"
and   `fitParam.0.batchSize`="32"
and   `fitParam.0.featureSize`="${featureSize}"
and   `fitParam.0.labelSize`="41"
and   `fitParam.0.alg`="FCClassify"
and   validateTable="validateTable"
and   `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
```

之后系统会将SKlearn或者Tensorflow的日志发送到Kafka topic名称为`William_training_msg`的主题中。
用户去消费这个数据，便能看到自己的专属日志。

当然你也可以进入Spark UI查看更多信息。