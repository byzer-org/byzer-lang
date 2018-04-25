StreamingPro现在也支持用XQL定义流式计算了

## Stream XQL

当你根据[编译文档](https://github.com/allwefantasy/streamingpro/blob/master/docs/compile.md) 编译以及运行StreamingPro Service后，
你就可以通过一些http接口提交流式任务了。

通过接口 http://ip:port/run/script

post 参数名称为：sql

具体脚本内容为：

```
-- 设置该流式任务的名字。这个名字需要保持全局唯一。
set streamName="streamExample"

-- 加载kafka数据， 如果是0.8,0.9 那么分别使用kafka8 kafka9。如果是1.0 则
-- 直接使用kafka即可。指的注意是参数遵循原有spark规范，比如1.0参数你需要查看spark
-- 相关文档

load kafka9.`` options `kafka.bootstrap.servers`="127.0.0.1:9092"
and `topics`="testM"
as newkafkatable1;


-- 简单获得该周期的所有卡夫卡数据，不做任何处理。
select * from newkafkatable1
as table21;

-- 把数据增量保存到hdfs上，并且设置为json格式。 运行周期是10s,ck目录是 /tmp/cpl2
save append table21  
as json.`/tmp/abc2` 
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/cpl2";

```

提交该任务后，等待几秒钟，就可以通过接口

通过接口：

http://ip:port/stream/jobs/running
查看所有流式计算任务。

通过接口：

http://ip:port/stream/jobs/kill?groupId=....
可以杀死正在运行的流式任务

当然，你也可以打开SparkUI查看相关信息。