# SQL片段模板的使用

设置两个模板：

```shell
set bytesTpl = '''
concat(byteStringAsMb(concat(NVL({},0),"b")),"M")
''';

set recordsTpl = '''
concat(NVL({},0)/1000,"K")
''';
```

其中 "{}" 为占位符。

使用方式：

```sql
select 
${template.get("bytesTpl","sum(shuffle_bytes_written)")} as shuffleBytesWrite,
${template.get("bytesTpl","sum(shuffle_remote_bytes_read)+sum(shuffle_local_bytes_read)")}  as shuffleBytesRead,
${template.get("recordsTpl","sum(shuffle_records_written)")} as shuffleRecordsWrite,
${template.get("recordsTpl","sum(shuffle_records_read)")}  as shuffleRecordsRead
from w_job where group_id="5702c1fe-4749-4309-9c6a-ee739bd8006f" and created_at > timeAgo("6s") as jobs;
```

template.get 的第一个参数表示我要使用的模板名称，后面可以有一个或者多个参数，他会按顺序替换模板里面的"{}"占位符。

比如 `${template.get("bytesTpl","sum(shuffle_bytes_written)")}`最终对应的字符串是：

```sql
concat(byteStringAsMb(concat(NVL(sum(shuffle_bytes_written),0),"b")),"M")
```

当一个语句大量用统一模板，那么修改模板就使得处处生效，有效的提高了生产效率。里面的参数也支持位置引用参数。比如第一个你可以改成：

```shell
set bytesTpl = '''
concat(byteStringAsMb(concat(NVL({0},0),"b")),"M")
''';
```

`{0}` 表示会使用第一个参get方法里的第二个参数填充（第一个参数是模板名称）数进行填充。
