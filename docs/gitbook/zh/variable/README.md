# 变量

变量设置在MLSQL中是一个相当重要的语法，就和其他语言一样。 在MLSQL中，变量的申明如下：

```sql
set email="allwefantasy@gmail.com";
```

接着你就可以在后续脚本里使用 `${}`语法进行引用。

比如：

```sql
select "${email}" as email  as table1;
```

这里需要注意`${email}`需要被双引号括上，如果没有双引号，做完字面替换后会变成

```
select allwefantasy@gmail.com as email  as table1;
```
这显然是一个错误的sql语句。

## 嵌套变量

变量也是可以嵌套变量的，比如：

```sql
set email="allwefantasy@gmail.com";
set hellow="你好 ${email}";
```

## 内置变量引用
MLSQL 内嵌了几个变量：

1. HOME
2. OWNER
3. date

HOME是当前主目录，OWNER是当前执行该脚本的用户，date 是一个日期格式化对象。下面一段是日期格式化的使用示例

```sql
-- 获取当前日期变量
set day_id ='''${date.toString("yyyy-MM-dd")}''';
select "${day_id}" as a as test111;
```

在这里我们看到了最简单的变量使用方式，但实际上在MLSQL中变量非常强大，它支持如下几种模式：

1. string, 也就是我们上面看到的
2. conf 
3. shell
4. sql

下面章节我们一一介绍。