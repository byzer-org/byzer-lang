# ! 宏语法

宏语法的设计初衷是为了方便脚本片段的复用。

## 基本语法

假设我封装了一个根据用户名查询用户的功能逻辑，然后我希望将他方便的提供给其他人使用，
我们可以按如下方式写：

```sql
set findEmailByName = '''
-- notice that there are no semicolon in the end of this line. 
select email from table1 where name="{}" 
''';

-- Execute it like a command.
!findEmailByName "jack";
```

我们先通过set语法包含了一些MLSQL脚本片段，这里是一条SQL语句。接着我只要通过将感叹号应用于变量名称之上，我们就能将其作为一个命令执行。
这个例子，我们还需要传参数，可以使用"{}" 或者"{数字}"来做占位符。

## 内置的宏变量

我们内置了非常多的宏变量，比如对HDFS进行管理的 !hdfs, 以及对python做支持的!python。我们在后续的章节会会对这些命令有更多的讲解。