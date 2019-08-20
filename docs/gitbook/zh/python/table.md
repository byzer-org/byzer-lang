# 使用Python处理MLSQL中的表

在前面章节，我们提到了可以交互式使用python。但是因为他本身不是分布式的，所以性能会比较差。我们还提供了专门使用Python处理MLSQL中表的能力。

第一步，我们模拟一张表：

```sql
select 1 as a as table1;
```

第二步，设置环境以及Python处理后返回的表格式：

```sql
!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(a,integer),field(b,integer))";
```

第三步，书写Python代码：

```sql
!python on table1 '''
import pandas as pd
import numpy as np
for item in data_manager.fetch_once():
    print(item)
df = pd.DataFrame({'AAA': [4, 5, 6, 8],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
data_manager.set_output([[df['AAA'],df['BBB']]])

''' named mlsql_temp_table2;
```

这里，表示对table1使用python进行处理，处理后，我们通过data_manager.set_output方法设置返回值，返回值需要符合我们前面定义的Schema. 因为MLSQL
内部使用Apache Arrow作为数据交互格式，python返回给MLSQL的本质是列式格式。比如上面的例子：

```python
[[df['AAA'],df['BBB']]]
```

有两层List，最里面的一层list表示有两列。然后我们前面的schema就是描述这两列的类型以及名称。最后处理完成后的数据我们叫表mlsql_temp_table2.

现在，你可以获取结果数据看看：

```sql
select * from mlsql_temp_table2 as output;
```

值得注意的是：

1. 我们可以通过data_manager.fetch_once 获取table1里的数据，通过data_manager.set_output 返回数据。这期间会有多个python进程，每个python进程
只处理其中一部分数据。也就是数据其实是分布式处理的，大家不要以为是在一个进程里。
2. 很多莫名其妙的错误都是因为schema描述错误，大家需要谨慎。
3. 返回的格式必须是[[pd.serial1,pd.serial2....]] 格式。 