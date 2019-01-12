# window/watermark的使用

window/watermark是流式计算里特有的概念，下面是一个具体的使用模板：

```sql
select ts,f1 as table1;

-- register watermark for table1
register WaterMarkInPlace.`table1` as tmp1
options eventTimeCol="ts"
and delayThreshold="10 seconds";

-- process table1
select f1,count(*) as num from table1
-- window size is 30 minutes,slide is 10 seconds
group by f1, window(ts,"30 minutes","10 seconds")
as table2;

save append table2 as ....
```

得到一张表之后，你可以通过WaterMarkInPlace对该表进行watermark的设置，核心参数是 eventTimeCol 以及 delayThreshold，
分别设置evetTime字段和延时。

对于window没有特殊用法，只是一个函数。
