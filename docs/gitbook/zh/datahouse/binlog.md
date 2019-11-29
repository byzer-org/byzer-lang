# MySQL Binlog同步

MySQL得到了广泛的使用。数仓的一个核心点是需要将业务的数据库（离线或者实时）同步到数仓当中。离线模式比较简单，直接全量同步
覆盖，实时模式会略微复杂些。一般而言，走的流程会是：

```
MySQL -> Cannel(或者一些其他工具) -> Kafka -> 

流式引擎 -> Hudi(HBase)等能够提供更新存储的工具 -> 

同步或者转储 -> 对外提供服务  
```

我们看到，这是一个很繁琐的的流程。流程越长，总体出问题的概率就越高，我们调试也会越困难。 MLSQL提供了一个非常简单的解决方案：

```sql
MySQL -> MLSQL Engine -> Delta(HDFS)
```

用户的唯一工作是编写一个MLSQL代码，就可以直接运行于 MLSQL Console上。


整个脚本只包含两段代码，一个Load， 一个Save,令人惊讶的简单。

下面是Load语句：

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxx"
and password="xxxx"
and bingLogNamePrefix="mysql-bin"
and binlogIndex="4"
and binlogFileOffset="4"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;
```

set streamName 表名这是一个流式的脚本，并且这个流程序的名字是binglog. 
load语句我们前面已经学习过，可以加载任意格式或者存储的数据为一张表。这里，我们将MySQL binglog的日志加载为一张表。

值得大家关注的参数主要有两组，第一组是binglog相关的：

1. bingLogNamePrefix MySQL binglog配置的前缀。你可以咨询业务的DBA来获得。
2. binlogIndex 从第几个Binglog进行消费
3. binlogFileOffset 从单个binlog文件的第几个位置开始消费


binlogFileOffset并不能随便指定位置，因为他是二进制的，位置是有跳跃的。4表示一个binlogFileOffset的起始位置，是一个特殊的数字。
如果用户不想从起始位置开始，那么咨询DBA或者自己通过如下命令查看一个合理的位置：

```sql
mysqlbinlog \ 
--start-datetime="2019-06-19 01:00:00" \ 
--stop-datetime="2019-06-20 23:00:00" \ 
--base64-output=decode-rows \
-vv  master-bin.000004
```
如果随意指定了一个不合适的位置，展现出来的结果是数据无法得到消费，然后无法实现增量同步。

第二组参数是过滤哪些库的哪些表需要被同步：

1. databaseNamePattern  db的过滤正则表达式
2. tableNamePattern     表名的过滤正则表达式


现在我们得到了包含了binlog的table1,  我们现在要通过它将数据同步到Delta表中。这里一定需要了解，我们是同步数据，
而不是同步binglog本身。 我们将table1持续更新到delta中。具体代码如下：


```sql
save append table1  
as rate.`mysql_{db}.{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and syncType="binlog"
and checkpointLocation="/tmp/cpl-binlog2";
```

这里，我们对每个参数都会做个解释。

`mysql_{db}.{table}` 中的 db,table是占位符。因为我们一次性会同步很多数据库的多张表，如果全部手动指定会显得
非常的麻烦和低效。MLSQL的 rate 数据源允许我们通过占位符进行替换。

第二个是idCols， 这个参数已经在前面Delta数据库的章节中和大家见过面。idCols需要用户指定一组联合主键，使得MLSQL能够完成
Upsert语义。 

第三个syncType表示我们同步的是binlog,这样才会执行binlog特有的操作。

最后两个参数duration,checkpointLocation 则是流式计算特有的，分别表示运行的周期以及运行日志存放在哪。

现在，我们已经完成了我们的目标，将任意表同步到Delta数据湖中！

目前Binlog 同步有一些限制：

1. MySQL 需要配置 binlog_format=Row。 当然这个理论上是默认设置。
2. 只支持binglog中 update/delete/insert 动作的同步。如果修改了数据库表结构，默认会同步失败，用户需要重新全量同步之后再进行增量同步。
如果希望能够继续运行，可以在Save语句中设置mergeSchema="true"。
3. 如果不同的表有不同的主键列(需要配置不同的idCols),那么可能需要些多个流式同步脚本。

## 常见错误

如果一直出现

```
Trying to restore lost connectioin to .....
Connected to ....
```

那么看看MySQL的my.cnf中的server_id参数是不是有配置。
