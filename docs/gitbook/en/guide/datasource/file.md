# Parquet/Json/Text/Xml/Csv

MLSQL supports load file as table. You can use where condition in load statement to specify special
options. 

All value in where statement should be "string", you can use "" or ''' ''' to quote them.

Here are some examples:


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
