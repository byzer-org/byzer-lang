#如何高效AdHoc查询Kafka

虽然Kafka是一个流式数据源，但是将其作为普通的数据源进行AdHoc查询，性能也是比较可观的。
本章节我们会介绍如何用MLSQL AdHoc查询Kafka.

## 基本用法

使用上非常简单，你只要使用`adHocKafka` 数据源去加载Kafka即可：

```sql
load adHocKafka.`topic` where 
kafka.bootstrap.servers="127.0.0.1:9200"
and multiplyFactor="2" 
as table1;

select count(*) from table1 where value like "%yes%" as output;
```

这里multiplyFactor表示我们需要提升两倍并行度。基数是Kafka的分区数。设置为2，表示我们会启动两个线程扫描同一个分区，从而加快速度。我们还可以
设置开始截止时间：


```sql
load adHocKafka.`topic` where 

kafka.bootstrap.servers="127.0.0.1:9200"

and multiplyFactor="2" 

and timeFormat="yyyyMMdd"
and startingTime="20170101"
and endingTime="20180101"

as table1;

select cast(value as string) as textValue from table1 
as table2;

select count(*) from table2 where textValue like "%yes%" 
as output;
``` 

当然也可以直接指定offset区间：

```sql
load adHocKafka.`topic` where 

kafka.bootstrap.servers="127.0.0.1:9200"

and multiplyFactor="2" 

and staringOffset="oldest"
and endingOffset="latest"

as table1;
```

因为指定offset比较麻烦，我们一般都不怎么使用。因为你需要指定每个分区的起始结束offset。