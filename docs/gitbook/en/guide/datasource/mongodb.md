# MongoDB

The jar of hbase driver is not included by default. If you want to use this datasource, please 
make sure you have added the jar with `--jars` in startup command.

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

Notice the splitter in MongoDB is ".".

