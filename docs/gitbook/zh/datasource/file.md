# Parquet/Json/Text/Xml/Csv

MLSQL支持大部分HDFS/本地文件数据读取。对于数据的保存或者加载，后面都可以接where语句。
where语句的配置选项随着数据源的不同而不同个，比如csv格式需要设置是否保留header。一些共性
需求如保存后的文件数，可以同构fileNum等参数设置。

值得注意的是，where条件所有的value都必须是字符串，也就是必须用`"` 或者`'''`括起来。所有value可以使用set得到的
变量。下面是一些示例：


## Parquet
```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as parquet.`${savePath}`;

load parquet.`${savePath}` as table2;

```


## Json

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as json.`${savePath}`;

load json.`${savePath}` as table2;

```

## csv

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as csv.`${savePath}` where header="true";

load csv.`${savePath}` as table2 where header="true";

```


## text

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as json.`${savePath}`;

load text.`${savePath}` as table2;

```

## Xml

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

set savePath="/tmp/jack";

load jsonStr.`rawData` as table1;

save overwrite table1 as xml.`${savePath}`;

load xml.`${savePath}` as table2;

```
