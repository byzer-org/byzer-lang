# Change the partitions of table

In many situations, we need to change the partitions, e.g. before we save the data, we want the table will have only on file
or we use python, we hope the python worker should be more, and you can use RepartitionExt to help you achieve it.

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

run data as RepartitionExt.`` where partitionNum="2" 
as newdata;
```

And the table newdata now have only 2 partitions.

 
