#定义自己的命令

大家可以使用系统内置的一些命令,具体查看方式如下：

```sql
!show help;
``` 

此外还内置了如下两个指令：

```sql
!desc jdbc;
!kill 任务id;
```


用户也可以定制自己的指令，比如用户自己实现kill指令：

```sql
set kill = '''
-- 注意结尾没有分号
run command Kill.`{}`
''';
```

接着就可以使用`!`来使用kill指令了。

```sql
!kill jobId;
```