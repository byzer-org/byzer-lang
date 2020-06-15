# MLSQL Cheatsheet
快速脏手指南  

## 数据读写

### json格式
#### 读取  
```
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;
```
#### 保存  
```
save overwrite table1 as json.`${savePath}`;
```
### JDBC
#### 读取  
（配置url为hive、postgre等驱动，添加对应jar包，可读写相应数据）  
```
load jdbc.`db.table` options
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and user="..."
and password="...."
and prePtnArray = "age<=10 | age > 10"
and prePtnDelimiter = "\|"
as table1;
```
#### 保存   
（append可替换为overwrite、ignore、errorIfExists）
```
save append table1 as jdbc.`test.saved_table_name`;
```

### 流式数据写入到mysql  
```
save append outputTable  
as streamJDBC.`mydb.patient` 
options mode="update"
and `statement-0`="insert into patientUpdate(dt,addr,num) value(?,?,?) ON DUPLICATE KEY UPDATE dt=?,addr=?,num=?;"
and duration="5"
and checkpointLocation="/streamingpro-test/kafka/patient/mysql/update";
```

### 复用数据库连接配置
```
 set user="root";
 set password="password";
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as test;
 -- 使用配置读取mysql中db_1数据库的table表 ,directQuery在数据库端执行
 load jdbc.`test.table` where directQuery='''
 select * from table where a = "b"
 ''' as table1;
```  
### ElasticSearch读写
```
connect es where  `es.index.auto.create`="true"
and es.nodes="127.0.0.1" as es_instance;
load es.`es_instance/twitter/cool`
as table1;
save overwrite data1 as es.`twitter/cool` where
`es.index.auto.create`="true"
and es.nodes="127.0.0.1";

```

### Hbase读写  
```
connect hbase2x where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;
load hbase2x.`hbase1:mlsql_example` where field.type.label="DoubleType"
as mlsql_example ;
save overwrite orginal_text_corpus1 
as hbase2x.`hbase1:mlsql_example`;
``` 
### MongoDB读写 
```
connect mongo where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter" as mongo_instance;
load mongo.`mongo_instance/cool`
as table1;
save overwrite data1 as mongo.`twitter/cool` where
    partitioner="MongoPaginateBySizePartitioner"
and uri="mongodb://127.0.0.1:27017/twitter";
```
### Parquet  
```
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';
set savePath="/tmp/jack";
load jsonStr.`rawData` as table1;
save overwrite table1 as parquet.`${savePath}`;
load parquet.`${savePath}` as table2;
```
### csv  
```
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';
set savePath="/tmp/jack";
load jsonStr.`rawData` as table1;
save overwrite table1 as csv.`${savePath}` where header="true";
load csv.`${savePath}` as table2 where header="true";
```
### xml 
```
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';
set savePath="/tmp/jack";
load jsonStr.`rawData` as table1;
save overwrite table1 as xml.`${savePath}`;
load xml.`${savePath}` as table2;
```


## UDF
### 注册  
#### 方法一
```
set myUDF='''

def apply(a:Double,b:Double)={
   a + b
}

''';
load script.`myUDF` as scriptTable;
register ScriptUDF.`scriptTable` as plusFun;
```
#### 方法二
```
register ScriptUDF.`` as plusFun where
and lang="scala"
and udfType="udf"
code='''
def apply(a:Double,b:Double)={
   a + b
}
''';
```
### 使用 
```
set data='''
{"a":1}
{"a":1}
{"a":1}
{"a":1}
''';
load jsonStr.`data` as dataTable;
select plusFun(1,2) as res from dataTable as output;
```

## 调度

```
!scheduler "bigbox.main.mlsql" with "*/3 * * * * ";
```