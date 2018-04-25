## 如何使用Carbondata

如果要开启Carbondata支持，编译时需要加上 `-Pcarbondata` ，同时
启动时需要添加下面参数开启。当前carbondata 版本为 1.3.1。

```
-streaming.enableCarbonDataSupport true
-streaming.carbondata.store  "/data/carbon/store"
-streaming.carbondata.meta "/data/carbon/meta"
```

通过rest 接接口/run/sql 创建一张表：

```
CREATE TABLE carbon_table2 (
      col1 STRING,
      col2 STRING
      )
      STORED BY 'carbondata'
      TBLPROPERTIES('streaming'='true')

```

通过rest接口 /run/script提交一个流式程序：

```sql
set streamName="streamExample";
load kafka9.`-` where `kafka.bootstrap.servers`="127.0.0.1:9092"
and `topics`="testM"
as newkafkatable1;

select "abc" as col1,decodeKafka(value) as col2 from newkafkatable1
as table21;

save append table21  
as carbondata.`-` 
options mode="append"
and duration="10"
and dbName="default"
and tableName="carbon_table2"
and `carbon.stream.parser`="org.apache.carbondata.streaming.parser.RowStreamParserImp"
and checkpointLocation="/data/carbon/store/default/carbon_table2/.streaming/checkpoint";
```

通过rest接口 `/stream/jobs/running` 查看是否正常运行。

通过rest接口 `run/sql`查看结果表：

```sql
select * from carbon_table2
```

查询结果如下：

```json
[
    {
        "col1": "abc"
    },
    {
        "col1": "abc"
    },
    {
        "col1": "abc"
    },
    {
        "col1": "abc"
    },
    {
        "col1": "abc",
        "col2": "今天才是我的dafk"
    },
    {
        "col1": "abc",
        "col2": "dakfea"
    },
    {
        "col1": "abc",
        "col2": "afek"
    },
    {
        "col1": "abc",
        "col2": "dafkea"
    },
    {
        "col1": "abc",
        "col2": "dafkea"
    }
]
```


