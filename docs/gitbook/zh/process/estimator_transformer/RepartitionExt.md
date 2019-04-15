# 改变表的分区数

很多时候，我们需要改变分区数，比如保存文件之前，或者我们使用python,我们希望python worker尽可能的并行运行,这个时候就需要
RepartitionExt的帮助了。

具体使用案例：

```sql
set jsonStr = '''
{"id":0,"parentId":null}
{"id":1,"parentId":null}
{"id":2,"parentId":1}
{"id":3,"parentId":3}
{"id":7,"parentId":0}
{"id":199,"parentId":1}
{"id":200,"parentId":199}
{"id":201,"parentId":199}
''';

load jsonStr.`jsonStr` as data;

run data as RepartitionExt.`` where partitionNum=2 
as newdata;

```
