# Load 语法

Load的作用主要是为了方便加载数据源并且将其映射为一张表。我们知道，数据的来源可能是千差万别的，比如可能是流式数据源Kafka,也可能是传统的MySQL. 
所以我们需要一个一致的语法将这些数据源里的数据映射成MLSQL中的表。

## 基本使用

我们先来看一个最简单的load语句：

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;
```

这里，我们通过前面学习的set语法，设置了一个变量，该变量是abc, 然后，我们使用load语句将这段文本注册成一张视图（表）。运行
结果如下：

```
dataType  x         y   z

A         group	100	200	200
B         group	120	100	260

```

我们过来看下load语法。第一个关键词是load,紧接着接一个数据源或者格式名称，比如上面的例子是`jsonStr`。这表示我们要加载一个json的字符串，
在后面，我们会接.`abc`.通常``内是个路径。比如 csv.`/tmp/csvfile`，到这一步，MLSQL已经知道如何加载数据了，我们最后使用`as table1`,表示将数据
注册成视图（表），注册的东西后面可以引用。 比如在后续的select语句我们可以直接使用table1：

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;
select * from table1 as output;
```

我们可以获得和上面例子一样的输出。 load语句还可以接一些参数，比如

```sql
load jdbc.`db.table`  where url="jdbc:postgresql://localhost/test?user=fred&password=secret" 
as newtable;
```

这里，我们通过where条件里的url参数告诉jdbc格式，我们要连接的数据库地址，并且我们需要访问的路径是db数据库里的table表。最后，我们将对应的数据库表注册成
视图(表)newtable。

有的时候，我们需要多次书写load语句，比如我们需要load多张mysql表，那么每次都填写这些参数会变成一件让人厌烦的事情。

```sql
load jdbc.`db.table` options
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and user="..."
and password="...."
and prePtnArray = "age<=10 | age > 10"
and prePtnDelimiter = "\|"
as table1;
```

比如上面的配置就很多了。为了减少书写，我们引入一个特殊的配合load的语法，叫connect：


```sql
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as db_1;
```

connect 后面接数据格式，这里是jdbc, 然后后面接一些配置。最后将这个连接取一个名字，叫做db_1. 现在我们可以这样写load语句了：

```sql
load jdbc.`db_1.test1` where directQuery='''
select * from test1 where a = "b"
''' as newtable;

select * from newtable;
```

我们不再需要书写繁琐的配置。让我们再回顾下上面的语法：

1. load/connect 都需要需要最后接as， as 表示为注册，前面是注册的内容，as后面是注册后的名字。load as 是注册视图（表），connect as 是注册连接。
2. 注册的东西在后面的语句中都可以直接使用，这也是MLSQL能成为脚本的根基之一。

通用参数withoutColumns、withColumns，可以通过withoutColumns移除掉不需要的列，withColumns指定需要的列，两者只能选一个

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` options withoutColumns="x,y" as table1;
```      

Load也支持 conditionExpr，你可以加载数据的时候指定条件：

```sql
load hive.`test.test` options conditionExpr="dt=20190522" as t;
```


## 如何高效的书写Load语句

从上面的例子，我们可以看出，使用load语句，前提是我们知道有哪些数据源或者格式可用。还有一个难题，就是这些数据源或者格式都有什么可选的配置，也就是我们
前面看到的where条件语句。 MLSQL Console提供了图形化的界面供我们选择和填写参数：

![](http://docs.mlsql.tech/upload_images/WX20190819-152205@2x.png)

填写完成后，MLSQL会自动帮我们生成语句。

可能有的用户并不会使用Console,那么除了文档以外，我们还有办法去探索有哪些可用的数据源以及响应的配置么？ MLSQL 还提供了一些帮助指令。

```sql
!show datasources;
```

通过该命令，你可以看到大部分可用数据源。 接着，你找到你需要的数据源名称，然后通过下面的命令，获得这个数据源的参数以及文档(这里我们以CSV为例)：

```sql
!show "datasources/params/csv";
```

大家可以看到CSV可用参数比较详细的解释：

![](http://docs.mlsql.tech/upload_images/WX20190819-152658@2x.png)

我们在后续章节中，会对常见的数据源有更详细的介绍。
