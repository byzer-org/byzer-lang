# run语法

run和后面train语法形态上是完全一致的。只是train关键字更适合机器学习里面的`训练`语义。 而run适合做数据处理。
所以通常而言，run可以看做是transformer,而train则是estimator.

我们来看看run的语法形态：

```
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

run data as TreeBuildExt.`` 
where idCol="id" 
and parentIdCol="parentId" 
and treeType="nodeTreePerRow" 
as result; 

```

我们重点看最后一句的构成，对data数据集，使用 TreeBuildExt进行处理，处理过程中通过where条件来配置一些运作的参数，最后运作的结果
会产生一张新表，我们叫做result表。

所以无论run还是train,其实都是适用于我们后面会介绍的estimator/transformer的。他和传统的SQL并不是一致的，是MLSQL特有的语法。

在上面的例子，输出结果是这样的：

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

计算的其实是父子关系。这个我们在后面详细的 `estimator/transformer`章节也会介绍。 

所以在使用run/train之前，我们需要知道有哪些`estimator/transformer`可用。第一是可以参看文档，第二是可以使用load指令查看：

```sql
load modelList.`` as output;
```

这会列出所有可用`estimator/transformer`。如果你只看某个具体的模块，可以这样：


```sql
load modelList.`` as models;
select * from models where name like "%Tree%"  as output;
```

这个时候会只有TreeBuildExt被显示。你还可以看看它都有哪些运行参数：

```sql
load modelParams.`TreeBuildExt`  as output;
```

你就可以看到所有参数列表和说明了。

```
param                               description
--------------------------------------------------
idCol                               (undefined)
parentIdCol                         (undefined)
recurringDependencyBreakTimes       default:1000 the max level should lower than this value; When travel a tree, once a node is found two times, then the subtree will be ignore (undefined)
topLevelMark                        (undefined)
treeType treePerRow|nodeTreePerRow  (undefined)
```