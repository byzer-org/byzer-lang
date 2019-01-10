# MongoDB

MongoDB 是一个应用很广泛的存储系统。MLSQL也支持将其中的某个索引加载为表。

注意，MongoDB，所以你需要通过 --jars 带上相关的依赖才能使用。

## 加载数据

示例：

```sql
set data='''
{"jack":"cool"}
''';

load jsonStr.`data` as data1;

save overwrite data1 as mongo.`twitter/cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter";

load mongo.`twitter/cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter"
as table1;
select * from table1 as output1;

connect mongo where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter" as mongo_instance;

load mongo.`mongo_instance/cool`
as table1;
select * from table1 as output2;

load mongo.`cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter"
as table1;
select * from table1 as output3;
```

在MongoDB里，数据连接引用和表之间的分隔符不是`.`,而是`/`。

