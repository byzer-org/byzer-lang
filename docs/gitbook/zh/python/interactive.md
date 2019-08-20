#交互式Python

在MLSQL中，任何用户都可以开启一个独有的Python Server,从而完成一系列调试。


> 使用前，请先在环境中安装pyjava. 尝试使用 pip install pyjava命令。
> pyjava会提供一个叫data_manager的变量，方便接受和返回数据给MLSQL主程序。
> 主要有两个方法：
>    获取数据， data_manager.fetch_once(), 返回一个迭代器，注意，该方法只能调用一次。
>    设置返回数据， data_manager.set_output(value) value格式必须是 [[pandas.serial,pandas.serial,...]]




首先，我们要开启一个Python Server,这通过在MLSQL Console的编辑区输入如下指令完成：

```sql
!python start;
```

第二步，我们需要配置一些环境变量，包括使用什么环境，以及配置数据返回的是什么格式：

```sql
!python env "PYTHON_ENV=source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(a,integer),field(b,integer))";
```

在上面的的代码中，我选择使用一个叫`streamingpro-spark-2.4.x`的conda虚拟环境。如果不使用虚拟环境，则可设置为`:`,这样：

```sql
!python env "PYTHON_ENV=:";
```

然后我说我接下来从python返回过来的数据的格式是如下类型的：

```
st(field(a,integer),field(b,integer))
```

因为在MLSQL中，任何数据都是以表的形式存在，python返回给MLSQL的数据也必须能被描述成表的形式。我们提供了一个非常简单的的描述Schema形式的语言，详情
可参看[这个项目](https://github.com/allwefantasy/simple-schema)。

第三步，我们需要引入一些Python的包：

```sql
!python '''
import pandas as pd
import numpy as np
''';
```

最后我返回一些数据展示在MLSQL Console上：

```sql
!python  '''
df = pd.DataFrame({'AAA': [4, 5, 6, 8],'BBB': [10, 20, 30, 40],'CCC': [100, 50, -30, -50]})
data_manager.set_output([[df['AAA'],df['BBB']]])
''';
```

如果用户不准备使用了，可以关闭这个Python Server:

```sql
-- !python close;
```


