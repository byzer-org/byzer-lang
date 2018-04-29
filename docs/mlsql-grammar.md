## MLSQL语法支持

MLSQL支持如下几种语法：


### load 语法

```sql
load es.`index/type` 
as table;
```

load 语法主要是为了能够把一些外部存储，比如es,mysql等映射成StreamingPro里的表。load过程中可能需要有一些配置信息，可以通过options参数解决

比如：

```
load es.`index/type` options  `es.nodes`="127.0.0.1"
as table;
```

### select 语法

select 语法支持的是spark sql,如果你开启了hive支持，也是支持hive 的select 语法的。典型用法如下：
 
```sql

select * from db.abc
as table;
```

和传统select 语句唯一的差别就是后面有一个as关键字，该字表示这条sql查询得到的结果会被命名为表`table`,方便在后续的sql中引用。
有这个功能（临时表功能），也让你可以避免写非常复杂的sql。

## save 语法

save 语法主要完成持久化功能。 比如我要把一张hive表保存成parquet格式：

```sql

select * from hivedb.abc
as table;

save overwrite table as parquet.`/tmp/table`;
```

如果需要配置参数去控制保存的方式，则可以通过如下指令完成：


```sql

select * from hivedb.abc
as table;

save owerwrite table as parquet.`/tmp/table` options  truncate="true";
```

值得注意的是，save 后面紧临的关键字是 overwrite | append | errorIfExists |ignore。接着通过as 后面指定存储的格式和目录，最后通过
options参数配置存储动作。

### connect语法

```sql
connect jdbc where driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
as tableau;
```

connect 语法主要是让StreamingPro Server记住数据库某个DB的连接信息，避免在后续脚本中反复填写。connect 语法除了支持mysql,也支持其他需要
url地址，账号密码等体系的存储，比如ES等。

在上面的示例代码中，如果你后续想操作MySQL数据库里wow DB,你可以使用`tableau` 进行引用了。

比如我们加载一张mysql表，可以使用如下语法：

```sql
load jdbc.`tableau.table` 
as table2;

select * from table2 
as table3;
```

或者说我们要把数据保存到mysql里：

```
select "a" as a,"b" as b
as abc;

-- 这里只是表名你是可以使用上面的表形成一张新表
select * from abc
as newabc;

save overwrite newabc
as jdbc.`tableau.table`
options truncate="true"
```

### insert|create

insert/create 语法和hive完全保持一致。

### set 语法

set 语法可以使得SQL脚本更加动态。比如我希望写入到一个目录名是时间的目录，可以这么做：

```sql
set  xx = `select unix_timestamp()` options type = "sql" ;
select '${xx}'  as  t as tm;
save overwrite tm as parquet.`hp_stae=${xx}`;
```

set 支持type 类型为 sql|shell|conf。 如果什么都不配置，则单做简单的字符串处理。

当为sql 类型时， 支持标准的select 语法。 

当为shell语法时，会按shell脚本的方式执行，比如：

```sql
set yesterdayM = `date -v-1d +%Y-%m-%d` options type = "shell" ;
select  array(1,2,3)  as t as tm;
save overwrite tm as parquet.`/tmp/tableName/hp_stat_date=${yesterday}`;
```

当为conf时，也会当普通字符串处理，但是会额外用spark.sql运行下。比如有时候配置一些hive参数什么的，非常有用。
对于set 变量的使用方式：

1. 在set语法里，可以使用shell命令行，需要使用反引号。如果是简单字符串请用 双引号括起来。
2. 在save语句的path里，可以使用 ${占位符}，执行时会被替换。
3. 在select 语句里，可以使用${占位符语法}
4. 在load语句里，可以在加载的表或者路径中使用${占位符语法}
5. 默认${yesterday},${today},${tomorrow} ${theDayBeforeYesterday} 等都是可以不适用set直接使用的。格式为 yyyy-MM-dd

### train 语法（算法使用）


```sql
-- 从tableName获取数据，通过where条件对Algorithm算法进行参数配置并且进行模型训练，最后
-- 训练得到的模型会保存在path路径。
train [tableName] as [Algorithm].[path] where [booleanExpression]
```

比如：

```sql
train data as RandomForest.`/tmp/model` where inputCol="featrues" and maxDepth="3"
```

这句话表示使用对表data中的featrues列使用RandomForest进行训练，树的深度为3。训练完成后的模型会保存在`tmp/model`。

### register 语法


```sql
-- 从Path中加载Algorithm算法对应的模型，并且将该模型注册为一个叫做functionName的函数。
register [Algorithm].[Path] as functionName;
```

比如：

```
register RandomForest.`/tmp/zhuwl_rf_model` as zhuwl_rf_predict;
```

接着我就可以在SQL中使用该函数了：

```
select zhuwl_rf_predict(features) as predict_label, label as original_label from sample_table;
```

很多模型会有多个预测函数。假设我们名字都叫predict

LDA 有如下函数：

* predict  参数为一次int类型，返回一个主题分布。
* predict_doc 参数为一个int数组，返回一个主题分布

对应的`隐含`函数，大家看具体的实现文档。

### create / insert 语法

这两个语法完全兼容Hive,并且这两个语句都支持set语法的里的变量，比如：

```
set tableName="abc";
create table ${tableName}.... 
```
