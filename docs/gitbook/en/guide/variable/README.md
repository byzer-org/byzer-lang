# Variable

Variable is very important in a language. In MLSQL , variable declaration is like this:

```sql
set email="allwefantasy@gmail.com";
```

then you can reference the email variable with `${}`, for example:

```sql
select "${email}" as email  as table1;
```

Notice that the ${email} is quoted by `"`, if not, the final result should like this:

```sql
select allwefantasy@gmail.com as email  as table1;
```

and this SQL is definitely wrong.

# Nested Variable

You can reference a variable in a variable declaration:

```sql
set email="allwefantasy@gmail.com";
set hellow="你好 ${email}";
```

# Built-in Variable

1. HOME
2. OWNER
3. date

HOME is you home directory, and OWNER means who execute the script, date is date object.

```sql

-- 获取当前日期变量
set day_id ='''${date.toString("yyyy-MM-dd")}''';
select "${day_id}" as a as test111;
```

MLSQL also supports several other types of `set`:

1. string, as mentioned before.
2. conf
3. shell
4. sql

We will show the detail of them in next chapter. 