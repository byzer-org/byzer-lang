# MLSQL-Watcher插件使用

MLSQL-Watcher 可以收集一些关键数据到MySQL,然后可以通过MLSQL脚本计算对应的指标从而判别
一个MLSQL脚本是不是危险。具体的文章参考：[如何实现Spark过载保护](https://zhuanlan.zhihu.com/p/112608353)

## 开启方式

注册一个MySQL数据库即相当于开启收集功能:

```shell
!watcher db add "app_runtime_full" '''
app_runtime_full:
      host: 127.0.0.1
      port: 3306
      database: app_runtime_full
      username: xxxx
      password: xxxxxx
      initialSize: 8
      disable: false     
      testWhileIdle: true      
      maxWait: 100
      filters: stat,log4j
''';
```

此时MLSQL会连接该数据库，并且每两秒收集一次数据写入MySQL.


## 设置日志保留时间
我们还需要设置MySQL的清理时间，避免MySQL数据集过大：

```shell
!watcher cleaner 7d;
```

比如我这里设置保留七天的记录。支持秒(s),分钟(m),小时(h)等后缀。

## 加载数据表

你可以加载相关MySQL库表到你的MLSQL Console里：

```sql
connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/app_runtime_full?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="xxxxx"
 and password="xxxxxx"
 as app_runtime_full;
 
load jdbc.`app_runtime_full.w_executor` as w_executor;
load jdbc.`app_runtime_full.w_executor_job` as w_executor_job;
```

之后就可以查询统计这些信息了。

下面一段脚本是针对每个executor计算一个特定MLSQL脚本的危险指数：

```sql
------------------------------------------------------------------
参考文章：https://zhuanlan.zhihu.com/p/112608353
--------------------------------------------------------------------------------

-- 规整数据
select cluster_name,executor_name,group_id,
(shuffle_local_bytes_read+shuffle_remote_bytes_read) as reads,
shuffle_bytes_written as writes,
shuffle_records_read as records_read,
shuffle_records_read as records_writes,
created_at from 
w_executor_job order by created_at as temp1;

--找到最新的一次快照
select *, if((max(created_at) over())=created_at,0,1) as wow from  temp1  where group_id="4b933c30-8f16-4cca-bac7-bc996d03b2fb"  as temp2;

-- 计算平均值
select executor_name,reads+writes as rs,avg(reads) over () +avg(writes) over () as avg_rw from temp2 where wow=0 as temp3;

-- 计算每个exeuctor 的不均衡值，此处每个节点只有一条记录
select executor_name,(rs-avg_rw)/(3*avg_rw) as rw_inbalance from temp3 as temp4;

-- 计算 shuffle 速率，我们假设最大值为 1000M 每秒 对于一个节点
select *, (lag(reads,1) over (order by created_at)) as b_reads,(lag(writes,1) over (order by created_at)) as b_writes
from temp1  where group_id="4b933c30-8f16-4cca-bac7-bc996d03b2fb" 
as temp5;
select executor_name,((reads-b_reads) + (writes-b_writes))/(1000*1024*1024) as speed from temp5 as temp5;

-- 计算记录大小 单记录最大100m
select executor_name,reads/records_read/(100*1024*1024) as record_size from temp2 where wow=0 as temp6;

-- gc 时间 
select name as executor_name,gc_time, 
if((max(created_at) over())=created_at,0,1) as wow,
(lag(gc_time,1) over (order by created_at)) as b_gc_time 
from w_executor 
as temp7;
select *,(gc_time-b_gc_time) as gc_s from temp7 where wow=0 as temp7;

-- 最后根据executor join 起来，得到四个因子
select temp4.executor_name,temp4.rw_inbalance,temp5.speed,temp6.record_size,temp7.gc_s 
from temp4 
left join temp5 on temp4.executor_name = temp5.executor_name
left join temp6 on temp4.executor_name = temp6.executor_name
left join temp7 on temp7.executor_name = temp4.executor_name
as output;
```



