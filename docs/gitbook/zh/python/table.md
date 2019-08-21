# 使用Python处理MLSQL中的表

在前面章节，我们提到了可以交互式使用python。但是因为他本身不是分布式的，所以性能会比较差。我们还提供了专门使用Python处理MLSQL中表的能力。

> 使用前，请先在环境中安装pyjava. 尝试使用 pip install pyjava命令。
> pyjava会提供一个叫data_manager的变量，方便接受和返回数据给MLSQL主程序。
> 主要有两个方法：
>    获取数据， data_manager.fetch_once(), 返回一个迭代器，注意，该方法只能调用一次。
>    设置返回数据， data_manager.set_output(value) value格式必须是 [[pandas.serial,pandas.serial,...]]

第一步，我们模拟一张表：

```sql
select 1 as a as table1;
```

第二步，设置环境以及Python处理后返回的表格式：

```sql
!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(a,long),field(b,long))";
```
在上面的的代码中，我选择使用一个叫`streamingpro-spark-2.4.x`的conda虚拟环境。如果不使用虚拟环境，则可设置为`:`,这样：

```sql
!python env "PYTHON_ENV=:";
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

如果我么从data_manager.fetch_once()得到的结果集非常大，没办法放到内存中怎么办？ 可以分批次写，比如每循环100条，构成一个pd,然后将pd案列转化为一个
pd.serial list,多次则用yield 形成一个generator,最后将generator设置给 output即可。 一个示意代码如下：

```python
buffer = []

dm = data_manager

def gen():
    for item in dm.fetch_once():
        buffer.append(item)        
        if len(buffer)==100:
            yield convert_buffer_to_pd_pd_serial_list
        .....

# convert_buffer_to_pd_pd_serial_list 将buffer专户类似[df['AAA'],df['BBB']]结构    
data_manager.set_output(gen())
```

因为 generator是lazy的，所以其实每次我们都是处理一小批数据，然后写入到输出流里。

为了让大家看的更清楚，我手动构建了一个generator,大家可以看看格式要求：

```sql
select 1 as a as table1;

!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(a,long),field(b,long))";

!python on table1 '''
import pandas as pd
import numpy as np

for item in data_manager.fetch_once():
      print(item)
      
df = pd.DataFrame({'AAA': [4, 5, 6, 8],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})

def wow(df):
   yield [df['AAA'],df['BBB']]
   yield [df['CCC'],df['CCC']]
   
data_manager.set_output(wow(df))

''' named mlsql_temp_table2;

select * from mlsql_temp_table2 as output;
```

值得注意的是：

1. 我们可以通过data_manager.fetch_once 获取table1里的数据，通过data_manager.set_output 返回数据。这期间会有多个python进程，每个python进程
只处理其中一部分数据。也就是数据其实是分布式处理的，大家不要以为是在一个进程里。
2. 很多莫名其妙的错误都是因为schema描述错误，大家需要谨慎。
3. 返回的格式必须是[[pd.serial1,pd.serial2....]] 格式。 

## 关于使用Python处理MLSQL表的一些原理

当我们使用Python处理一张表的数据时，我们会在表分区的节点启动相应的Python Workers,并且我们会一直复用他们。
对于这些Python Workers，我们可以通过如下方式限制Worker的大小。

```sql
!python conf "py_executor_memory=600";
```

上面的例子表示我们会限制Python的worker内存大小不超过600m.
值得注意的是，无论`!python env`还是`!python conf`都是session级别有效的。这意味着一旦设置之后，会影响当前用户后续所有的操作。
