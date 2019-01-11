# SQL模式

SQL模式可以充分利用一些SQL内置或者自己开发的UDF函数的优势。

比如我要获取日期可以这么搞：

```sql
set day_id=`select current_date()` options type="sql";
select "${day_id}" as a as test111;
```

当然，你也可以自己写段scala或者python脚本注册成udf函数，然后赋给变量，也是不错的选择：

```sql

-- 自定义 scala 的 UDF 方法

register ScriptUDF.`` as welcome
where lang="scala"
and code='''
def apply(str:String)={
    str
}
''';

set welcome_msg=`select welcome("hellow") as k` options type="sql";
select "${welcome_msg}" as a as output;

```

其实可以实现很多复杂场景的功能，大家可以根据自己的实际情况灵活使用。
