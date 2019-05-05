#sql

This means the variable is assigned by the result of executing sql. For example:

```sql
set day_id=`select current_date()` options type="sql";
select "${day_id}" as a as test111;
```

You can also create your own UDF then used in set statement.


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

If you have enabled the Auth, then sql will fail.

```sql
select 1 as a as table1;

set abc=`select a from table1` where type="sql";

select ${abc} from table1 as output;
```

Cause the auth is work in compile time, and the table1 is not exists yet, so the system will 
throws a exception. To avoid this, you can set the mode to `runtime`. Try this:

```sql
select 1 as a as table1;

set abc=`select a from table1` where type="sql" and mode="runtime";

select ${abc} from table1 as output;
```

But you can not use like this:

```sql
select 1 as a as table1;

set abc=`select a from table1` where type="sql" and mode="runtime";

select a from ${abc} as output;
```

variable abc should not be a table.
