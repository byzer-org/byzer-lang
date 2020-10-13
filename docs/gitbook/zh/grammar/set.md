# Set 语法

变量设置广泛的存在于各种语言中。MLSQL通过Set语法来完成变量设置。

## 基础应用

```sql
set hello="world";
```

此时你运行后不会有任何输出结果。因为他仅仅是设置了一个变量。那么怎么使用呢？

你可以在后续的语法中使用，比如：

```sql

set hello="world";

select "hello ${hello}" as title 
as output;
```

这里，我们引入了一个下个章节才会介绍的select语法，不过因为select语法相当简单，本质就是就是传统的SQL [select语句 + as + 表名]。大家现在只要记住这个规则就可以了。我们通过SQL语句将变量输出,得到结果如下：


```
title

hello world
```
通常， 变量可以用于任何语句的任何部分。甚至可以是结果输出表名，比如下面的例子


```sql

set hello="world";

select "hello William" as title 
as `${hello}`;

select * from world as output;
```

我们看到，我们并没有显式的定义world表，但是我们在最后一句依然可以使用，这是因为系统正确的解析了前面的${hello}变量。

因为表名是被语法解析格式限制的，所以我们需要用``将其括起来，避免语法解析错误。


值得一提的是，set语法当前的生命周期是request级别的，也就是每次请求有效。通常在MLSQL中，生命周期分成三个部分：

1. request （当前执行请求有效）
2. session  （当前会话周期有效）
3. application （全局应用有效）

我们后续章节会详细描述。

request级别表示什么含义呢？ 如果你先执行

```sql
set hello="world";
```

然后再单独执行

```sql
select "hello William" as title 
as `${hello}`;

select * from world as output;
```

系统会提示报错：

```
Illegal repetition near index 17
((?i)as)[\s|\n]+`${hello}`
                 ^
java.util.regex.PatternSyntaxException: Illegal repetition near index 17
((?i)as)[\s|\n]+`${hello}`
                 ^
java.util.regex.Pattern.error(Pattern.java:1955)
java.util.regex.Pattern.closure(Pattern.java:3157)
java.util.regex.Pattern.sequence(Pattern.java:2134)
java.util.regex.Pattern.expr(Pattern.java:1996)
java.util.regex.Pattern.compile(Pattern.java:1696)
java.util.regex.Pattern.<init>(Pattern.java:1351)
java.util.regex.Pattern.compile(Pattern.java:1028)
java.lang.String.replaceAll(String.java:2223)
tech.mlsql.dsl.adaptor.SelectAdaptor.analyze(SelectAdaptor.scala:49)
```

系统找不到${hello}这个变量,然后原样输出，最后导致语法解析错误。


## set语法类型

set 是一个相当灵活的语法。 在MLSQL中，根据作用的不同，我们将set区分为五种类型：

1. text
2. conf
3. shell
4. sql
5. defaultParam

第一种text也就是我们前面已经演示过的：

```sql
set hello="world";
```

第二种conf表示这是一个配置选项，通常用于配置系统的行为，比如 

```sql
set spark.sql.shuffle.partitions=200 where type="conf";
```

表示将底层spark引擎的shuffle默认分区数设置为200. 那么如何制定这是一个配置呢？ 答案就是加上

```sql
where type="conf"
```

条件子句。

第三种是shell,也就是set后的key最后是由shell执行生成的。 我们已经不推荐使用该方式。典型的例子比如：

```sql
set date=`date` where type="shell";
select "${date}" as dt as output;
```
注意，这里需要使用`` 括住该命令。
输出结果为：

```sql
dt

Mon Aug 19 10:28:10 CST 2019
```

第四种是sql类型，这意味着set后的key最后是由sql引擎执行生成的。下面的例子可以让大家看出其特点和用法：

```sql
set date=`select date_add(1) as set date=`select date_sub(CAST(current_timestamp() as DATE), 1) as dt` 
where type="sql";

select "${date}" as dt as output;
```

注意这里也需要使用`` 括住命令。 最后结果输出为：

```
dt

2019-08-18
```

最后一种是defaultParam，我们先看示例。

```sql
set hello="foo";
set hello="bar";

select "${hello}" as name as output;
```

最后输出是

```
name

bar
```

这符合大家直觉，下面的会覆盖上面的。那如果我想达到这么一种效果，如果变量已经设置了，我就不设置，如果变量没有被设置过，我就作为默认值。为了达到
这个效果，MLSQL引入了defaultParam类型：

```sql
set hello="foo";
set hello="bar" where type="defaultParam";

select "${hello}" as name as output;
```

最后输出结果是：

```
name

foo
```

如果前面没有设置过hello="foo",


```sql
set hello="bar" where type="defaultParam";

select "${hello}" as name as output;
```

则输出结果为

```
name

bar
```

## set语法编译时和运行时

MLSQL有非常完善的权限体系，可以轻松控制任何数据源到列级别的访问权限，而且创新性的提出了编译时权限，
也就是通过静态分析MLSQL脚本从而完成表级别权限的校验（列级别依然需要运行时完成）。

但是编译期权限最大的挑战在于set变量的解析，比如：

```sql
select "foo" as foo as foo_table;
set hello=`select foo from foo_table` where type="sql";
select "${hello}" as name as output; 
```
如果我们没有真的运行第一个句子，那么set语法就无法执行。但是因为是编译期，所以我们肯定不会真实运行第一个句子，这就会导致执行失败。
根本原因是因为set依赖了运行时才会产生的表。

为了解决这个问题，我们引入了 compile/runtime 两个模式。如果你希望你的set语句可以编译时就evaluate值，那么添加该参数即可。

```sql
set hello=`select 1 as foo ` where type="sql" and mode="compile";
```

如果你希望你的set值，只在运行时才需要，则设置为runtime:

```sql
set hello=`select 1 as foo ` where type="sql" and mode="runtime";
```

编译期我们不会运行这个set语句。


## 内置变量

MLSQL 提供了一些内置变量，看下面的代码：

```sql
set jack='''
 hello today is:${date.toString("yyyyMMdd")}
''';
```

date是内置的，你可以用他实现丰富的日期处理。

## 回顾

这个章节有点复杂，原因是因为set语法的灵活性已经功能很强大。但是用户只要掌握基本用法已经足够满足需求了。
