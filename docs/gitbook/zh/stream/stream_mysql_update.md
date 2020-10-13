# 如何使用MLSQL流式更新MySQL数据

很多场景会要求使用MLSQL把流式数据写入MySQL,而且通常都是upsert语义，也就是根据特定字段组合，
如果存在了则更新，如果不存在则插入。MLSQL在批处理层面，你只要配置一个idCol字段就可以实现upsert语义。
那么在流式计算里呢？

同样也是很简单的。 下面的例子来源于[用户讨论](https://github.com/allwefantasy/streamingpro/issues/919).

## 场景

有一个patient的主题，里面包含了

* name,string
* age,integer
* addr,string (means the province the patient from)
* arriveTime,long (timestamp in miliseconds)

最终我们要求根据addr,arriveTime作为uniq key，如果存在更新，否则insert,并且把数据存储在MySQL里。


## 流程

我们可以通过load语法把Kafka的topic加载成一张表：

```sql
set streamName="test_patient_count_update";

load kafka.`patient` options
`kafka.bootstrap.servers`="dn3:9092"
and `valueFormat`="json"
and `valueSchema`="st(field(name,string),field(age,integer),field(addr,string),field(arriveTime,string))"
as patientKafkaData;

```

因为kafka里的数据是json格式的，我希望直接展开成json表，所以配置了valueFormat和valueSchema，更多细节
参考前面的章节。

接着我要链接一个数据库：

```sql
connect jdbc where
url="jdbc:mysql://dn1:3306/streamingpro-cluster?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"
and driver="com.mysql.jdbc.Driver"
and user="XXX"
and password="XXXXX"
as mydb;

```

第一步我们考虑使用存储过程（以及相关的表）

```
set procStr='''
CREATE DEFINER=`app`@`%` PROCEDURE `test_proc`(in `dt` date,in `addr` VARCHAR(100),in `num` BIGINT)
begin 
	DECLARE cnt bigint;
	select p.num into cnt from patientUpdate p where p.dt=`dt` and p.addr=`addr`;
	if ISNULL(CNT) then 
		INSERT into patientUpdate(dt,addr,num) values(`dt`,`addr`,`num`);
	else 
		update patientUpdate p set p.num=`num` where p.dt=`dt` and p.addr=`addr`;
	end if;
end
''' ;

-- create table and procedure 创建table和存储过程
select 1 as a as FAKE_TABLE;
run FAKE_TABLE as JDBC.`mydb` where 
`driver-statement-0`="create table  if not exists patientUpdate(dt date,addr varchar(100),num BIGINT)"
and 
`driver-statement-1`="DROP PROCEDURE IF EXISTS test_proc;"
and
`driver-statement-2`="${procStr}"

```

接着对数据进行操作，获得需要写入到数据库的表结构：

```
select name,addr,cast(from_unixtime(arriveTime/1000) as date) as dt from patientKafkaData as patient;

select dt,addr,count(*) as num from patient
group by dt,addr
as groupTable;
```

最后，调用save语法写入：

```sql
save append groupTable
as streamJDBC.`mydb.patient` 
options mode="update"
-- call procedure 调用存储过程
and `statement-0`="call test_proc(?,?,?)"
and duration="5"
and checkpointLocation="/streamingpro-test/kafka/patient/mysql/update";
```

存储过程是个技巧。其实还有更好的办法：

```
select dt,addr,num, dt as dt1, addr as addr2 from groupTable as outputTable;

save append outputTable  
as streamJDBC.`mydb.patient` 
options mode="update"
and `statement-0`="insert into patientUpdate(dt,addr,num) value(?,?,?) ON DUPLICATE KEY UPDATE dt=?,addr=?,num=?;"
and duration="5"
and checkpointLocation="/streamingpro-test/kafka/patient/mysql/update";
```

通过`ON DUPLICATE KEY UPDATE`实现相关功能。 指的注意的是，statement-0 里是原生的sql语句，通过`?`占位符来设置参数，
我们会把待写入的表字段按顺序配置给对应的SQL语句。




