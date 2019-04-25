# Parent-Child relationship computing

It's difficult to compute the parent-child relationship with SQL. In most case, we have scenario as following：

1. Specify one node, get all child nodes of any level.
2. Specify one node, get the depth. 
3. Return one or more tree-like structure.

Suppose the data we will process is like this:

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
```

You can load this text with jsonStr:

```sql
load jsonStr.`jsonStr` as data;
```

Using ET TreeBuildExt to compute the relationship:

```sql
run data as TreeBuildExt.`` 
where idCol="id" 
and parentIdCol="parentId" 
and treeType="nodeTreePerRow" 
as result;
```

Here are the result:

```
+---+-----+------------------+
|id |level|children          |
+---+-----+------------------+
|200|0    |[]                |
|0  |1    |[7]               |
|1  |2    |[200, 2, 201, 199]|
|7  |0    |[]                |
|201|0    |[]                |
|199|1    |[200, 201]        |
|2  |0    |[]                |
+---+-----+------------------+
```

children includes nodes of all levels.

We can also compute the sub tree of evey node:

```sql
run data as TreeBuildExt.`` 
where idCol="id" 
and parentIdCol="parentId" 
and treeType="treePerRow" 
as result;
```

result:

```
+----------------------------------------+---+--------+-----+
|children                                |id |parentID|level|
+----------------------------------------+---+--------+-----+
|[[[], 7, 0]]                            |0  |null    |1    |
|[[[[[], 200, 199]], 199, 1], [[], 2, 1]]|1  |null    |2    |
+----------------------------------------+---+--------+-----+
```

children is a row, and you can use UDF to process it.

