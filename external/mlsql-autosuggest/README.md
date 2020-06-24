# SQL Code Intelligence
SQL Code Intelligence 是一个代码补全后端引擎。既可以作为MLSQL语法补全，
也可以为标准Spark SQL(Select语句)做补全。`

## 当前状态
【积极开发中，还未发布稳定版本】

## 目标
【SQL Code Intelligence】目标分成两个，第一个是标准SQL补全：

1. SQL关键字补全
2. 表/字段属性/函数补全
3. 可二次开发自定义对接任何Schema Provider

第二个是MLSQL语法补全(依托于标准SQL的补全功能之上)：

1. 支持各种数据源提示
2. 支持临时表提示（临时表字段补全等等）
3. 支持各种ET组件参数提示以及名称提示

对于表和字段补，函数补全，相比其他一些SQL代码提示工具，该插件可根据当前已有的信息精确推断。

## 效果展示

### 标准的SQL语法提示

```sql
select  no_result_type, keywords, search_num, rank
from(
  select  [鼠标位置] row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
  from(
    select jack1.*,no_result_type, keywords, sum(search_num) AS search_num
    from jack.drugs_bad_case_di as jack1,jack.abc jack2
    where hp_stat_date >= date_sub(current_date,30)
    and action_dt >= date_sub(current_date,30)
    and action_type = 'search'
    and length(keywords) > 1
    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
    --and no_result_type = 'indication'
    group by no_result_type, keywords
  )a
)b
where rank <=
```

鼠标在第三行第十列，此时系统会自动提示：
1. a [表名]
2. jack1展开的所有列
3. no_result_type
4. keywords
5. search_num


### 多行MLSQL的提示

```sql
load hive.`db.table1` as table2;
select * from table2 as table3;
select [鼠标位置] from table3 
```

假设db.table1 表的字段为a,b,c,d
其中鼠标在低3行第七列，在此位置，会提示：

1. table3
2. a
3. b
4. c
5. d

可以看到，系统具有非常强的跨语句能力，会自动展开*，并且推测出每个表的schema信息从而进行补全。

## MLSQL 数据源/ET组件参数提示

```sql
select spl  from jack.drugs_bad_case_di as a;
load csv.`/tmp.csv` where [鼠标位置]
```

通常加载csv我们需要设定下csv是不是包含header, 分割符是什么。不过一般我们需要去查文档才能知道这些参数。 现在，【SQL Code Intelligence】会给出提示：

```json
[
{"name":"codec","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"dateFormat","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"delimiter","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"emptyValue","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"escape","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"header","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"inferSchema","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}},
{"name":"quote","metaTable":{"key":{"table":"__OPTION__"},"columns":[]},"extra":{}}]
```



## 用户指南

### 部署

如果作为MLSQL插件运行， 请参考部署文档 [MLSQL部署](http://docs.mlsql.tech/zh/installation/)
该插件作为MLSQ默认插件，所以开箱即用

如果用户希望作为独立应用运行， 下载：[sql-code-intelligence](http://download.mlsql.tech/sql-code-intelligence/).
下载后 `tar xvf  sql-code-intelligence-0.1.0.tar` 解压,执行如下指令即可运行：

```
java -cp .:sql-code-intelligence-0.1.0.jar  tech.mlsql.autosuggest.app.Standalone  -config ./config/application.yml
```
application.yml 可以参考mlsql-autosuggest/config的示例。默认端口是9004.

### Schema信息

【SQL Code Intelligence】 需要基础表的schema信息，目前用户有三种可选方式：

1. 主动注册schema信息 （适合体验和调试）
2. 提供符合规范的Rest接口，系统会自动调用该接口获取schema信息 （推荐，对本项目无需任何修改）
3. 扩展【SQL Code Intelligence】的 MetaProvider，使得系统可以获取shcema信息。 (启动本项目时需要注册该类)

最简单的是方式1. 通过http接口注册表信息，我下面是使用scala代码完成，用户也可以使用POSTMan之类的工具完成注册。

```scala
def registerTable(port: Int = 9003) = {
    val time = System.currentTimeMillis()
    val response = Request.Post(s"http://127.0.0.1:${port}/run/script").bodyForm(
      Form.form().add("executeMode", "registerTable").add("schema",
        """
          |CREATE TABLE emps(
          |  empid INT NOT NULL,
          |  deptno INT NOT NULL,
          |  locationid INT NOT NULL,
          |  empname VARCHAR(20) NOT NULL,
          |  salary DECIMAL (18, 2),
          |  PRIMARY KEY (empid),
          |  FOREIGN KEY (deptno) REFERENCES depts(deptno),
          |  FOREIGN KEY (locationid) REFERENCES locations(locationid)
          |);
          |""".stripMargin).add("db", "db1").add("table", "emps").
        add("isDebug", "true").build()
    ).execute().returnContent().asString()
    println(response)
  }
```

创建表的语句类型支持三种:db,hive,json。 分别对应MySQL语法，Hive语法，Spark SQL schema json格式。默认是MySQL的语法。

接着就系统就能够提示了：

```scala
 def testSuggest(port: Int = 9003) = {
    val time = System.currentTimeMillis()
    val response = Request.Post(s"http://127.0.0.1:${port}/run/script").bodyForm(
      Form.form().add("executeMode", "autoSuggest").add("sql",
        """
          |select emp from db1.emps as a;
          |-- load csv.`/tmp.csv` where
          |""".stripMargin).add("lineNum", "2").add("columnNum", "10").
        add("isDebug", "true").build()
    ).execute().returnContent().asString()
    println(response)
  }
```

第二种在请求参数里传递searchUrl和listUrl,要求接口的输入输出需要符合`tech.mlsql.autosuggest.meta.RestMetaProvider`
中的定义。


第三种模式是用户实现一个自定义的MetaProvider，就可以充分利用自己的schema系统

```scala
trait MetaProvider {
  def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable]

  def list(extra: Map[String, String] = Map()): List[MetaTable]
}
```

使用时，在AutoSuggestContext设置下使其生效：

```
context.setUserDefinedMetaProvider(有的实现类的实例)
```

MetaTableKey 的定义如下：

```scala
case class MetaTableKey(prefix: Option[String], db: Option[String], table: String)
```

prefix是方便定义数据源的。比如同样一个表，可能是hive表，也可能是mysql表。如果你只有一个数仓，不访问其他数据源，那么设置为None就好。对于下面的句子：

```sql
load hive.`db.table1` as table2;
```
【SQL Code Intelligence】 会发送如下的MetaTableKey给你的MetaProvider.search方法：

```scala
MetaTableKey(Option(hive),Option("db"),Option("table2"))
```

如果是一个普通的SQL语句，而非MLSQL 语句，比如：

```sql
select * from db.table1
```

则发送给MetaProvider.search方法的MetaTableKey是这个样子的：

```scala
MetaTableKey(None,Option("db"),Option("table2"))
```

### 接口使用
访问接口： http://127.0.0.1:9003/run/script?executeMode=autoSuggest

参数1： sql SQL脚本
参数2： lineNum 光标所在的行号 从1开始计数
参数3： columnNum 光标所在的列号，从1开始计数

比如我用Scala写一个client:

```
object Test {
  def main(args: Array[String]): Unit = {
    val time = System.currentTimeMillis()
    val response = Request.Post("http://127.0.0.1:9003/run/script").bodyForm(
      Form.form().add("executeMode", "autoSuggest").add("sql",
        """
          |select spl  from jack.drugs_bad_case_di as a
          |""".stripMargin).add("lineNum", "2").add("columnNum", "10").build()
    ).execute().returnContent().asString()
    println(System.currentTimeMillis() - time)
    println(response)
  }

}
```

最后结果如下：

```json
[{"name":"split",
"metaTable":{"key":{"db":"__FUNC__","table":"split"},
"columns":[
{"name":null,"dataType":"array","isNull":true,"extra":{"zhDoc":"\nsplit函数。用于切割字符串，返回字符串数组\n"}},{"name":"str","dataType":"string","isNull":false,"extra":{"zhDoc":"待切割字符"}},
{"name":"pattern","dataType":"string","isNull":false,"extra":{"zhDoc":"分隔符"}}]},
"extra":{}}]
```
可以知道提示了split,并且这是一个函数，函数的参数以及返回值都有定义。

### 编程使用

创建AutoSuggestContext即可，然后用buildFromString处理字符串，使用suggest方法
进行推荐。

```scala

val sql = params("sql")
val lineNum = params("lineNum").toInt
val columnNum = params("columnNum").toInt

val sparkSession = SparkSession.builder().appName("local").master("local[*]").getOrCreate()
val context = new AutoSuggestContext(sparkSession,
  AutoSuggestController.mlsqlLexer,
  AutoSuggestController.sqlLexer)

JSONTool.toJsonStr(context.buildFromString(sql).suggest(lineNum,columnNum))
```

sparkSession也可以设置为null，但是会缺失一些功能，比如数据源提示等等。


## 开发者指南

### 解析流程
【SQL Code Intelligence】复用了MLSQL/Spark SQL的lexer，重写了parser部分。因为代码提示有其自身特点，就是句法在书写过程中，大部分情况下都是错误的，无法使用严格的parser来进行解析。

使用两个Lexer的原因是因为，MLSQL Lexer主要用来解析整个MLSQL脚本，Spark SQL Lexer主要用来解决标准SQL中的select语句。但是因为该项目高度可扩展，用户也可以自行扩展到其他标准SQL的语句中。

以select语句里的代码提示为例，整个解析流程为：

1. 使用MLSQL Lexer 将脚本切分成多个statement
2. 每个statement 会使用不同的Suggester进行下一步解析
3. 使用SelectSuggester 对select statement进行解析
4. 首先对select语句构建一个非常粗粒度的AST,节点为每个子查询，同时构建一个表结构层级缓存信息TABLE_INFO
5. 将光标位置转化为全局TokenPos
6. 将全局TokenPos转化select语句相对TokenPos
7. 根据TokenPos遍历Select AST树，定位到简单子语句
8. 使用project/where/groupby/on/having子suggester进行匹配，匹配的suggester最后完成提示逻辑

在AST树种，每个子语句都可以是不完整的。由上面流程可知，我们会以statement为粗粒度工作context,然后对于复杂的select语句，最后我们会进一步细化到每个子查询为工作context。这样为我们编码带来了非常大的便利。


### 快速参与贡献该项目
【SQL Code Intelligence】 需要大量函数的定义，方便在用户使用时给予提示。下面是我实现的`split` 函数的代码：

```scala
class Splitter extends FuncReg {

  override def register = {
    val func = MLSQLSQLFunction.apply("split").
      funcParam.
      param("str", DataType.STRING, false, Map("zhDoc" -> "待切割字符")).
      param("pattern", DataType.STRING, false, Map("zhDoc" -> "分隔符")).
      func.
      returnParam(DataType.ARRAY, true, Map(
        "zhDoc" ->
          """
            |split函数。用于切割字符串，返回字符串数组
            |""".stripMargin
      )).
      build
    func
  }

}
```

用户只要用FunctionBuilder去构建函数签名即可。这样用户在使用该函数的时候就能得到非常详尽的使用说明和参数说明。同时，我们也可以通过该函数签名获取嵌套函数处理后的字段的类型信息。

用户只要按上面的方式添加更多函数到tech.mlsql.autosuggest.funcs包下即可。系统会自动扫描该包里的实现并且注册。

### TokenMatcher工具类

在【SQL Code Intelligence】中，最主要的工作是做token匹配。我们提供了TokenMatcher来完成token的匹配。TokenMatcher支持前向和后向匹配。如下token序列:

```
select a , b , c from jack
```

假设我想以token index 3(b) 为起始点，前向匹配一个逗号，identify 可以使用如下语法：

```scala
val tokenMatcher = TokenMatcher(tokens,4).forward.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
```

接着你可以调用 tokenMatcher.isSuccess来判断是否匹配成功，可以调用tokenMatcher.get 获取匹配得到匹配成功后的index,通过tokenMatcher.getMatchTokens 获取匹配成功的token集合。

注意，TokenMatcher起始位置是包含的，也就是他会将起始位置的token也加入到匹配token里去。所以在上面的例子中，start 是4而不是3. 更多例子可以查看源码。

### 子查询层级结构

对于语句：

```sql
select  no_result_type, keywords, search_num, rank
from(
  select  keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
  from(
    select *,no_result_type, keywords, sum(search_num) AS search_num
    from jack.drugs_bad_case_di,jack.abc jack
    where hp_stat_date >= date_sub(current_date,30)
    and action_dt >= date_sub(current_date,30)
    and action_type = 'search'
    and length(keywords) > 1
    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
    --and no_result_type = 'indication'
    group by no_result_type, keywords
  )a
)b
where rank <=
```

形成的AST结构树如下：

```sql
select no_result_type , keywords , search_num , rank from ( select keywords , search_num , row_number ( ) over
 ( PARTITION BY no_result_type order by search_num desc ) as rank from ( select * , no_result_type , keywords ,
 sum ( search_num ) AS search_num from jack . drugs_bad_case_di , jack . abc jack where hp_stat_date >= date_sub (
 current_date , 30 ) and action_dt >= date_sub ( current_date , 30 ) and action_type = 'search' and length (
 keywords ) > 1 and ( split ( av , '\\.' ) [ 0 ] >= 11 OR ( split
 ( av , '\\.' ) [ 0 ] = 10 AND split ( av , '\\.' ) [ 1 ]
 = 9 ) ) group by no_result_type , keywords ) a ) b where rank <=


=>select keywords , search_num , row_number ( ) over ( PARTITION BY no_result_type order by search_num desc ) as
 rank from ( select * , no_result_type , keywords , sum ( search_num ) AS search_num from jack . drugs_bad_case_di
 , jack . abc jack where hp_stat_date >= date_sub ( current_date , 30 ) and action_dt >= date_sub ( current_date
 , 30 ) and action_type = 'search' and length ( keywords ) > 1 and ( split ( av ,
 '\\.' ) [ 0 ] >= 11 OR ( split ( av , '\\.' ) [ 0 ] = 10
 AND split ( av , '\\.' ) [ 1 ] = 9 ) ) group by no_result_type , keywords )
 a ) b


==>select * , no_result_type , keywords , sum ( search_num ) AS search_num from jack . drugs_bad_case_di , jack
 . abc jack where hp_stat_date >= date_sub ( current_date , 30 ) and action_dt >= date_sub ( current_date , 30
 ) and action_type = 'search' and length ( keywords ) > 1 and ( split ( av , '\\.' )
 [ 0 ] >= 11 OR ( split ( av , '\\.' ) [ 0 ] = 10 AND split
 ( av , '\\.' ) [ 1 ] = 9 ) ) group by no_result_type , keywords ) a
```

我们可以看到一共嵌套了两层，每层都有一个子查询。

对此形成的TABLE_INFO结构如下：

```
2:
List(
MetaTableKeyWrapper(MetaTableKey(None,Some(jack),drugs_bad_case_di),None), 
MetaTableKeyWrapper(MetaTableKey(None,None,null),Some(a)), 
MetaTableKeyWrapper(MetaTableKey(None,Some(jack),abc),Some(jack)))
1:
List(MetaTableKeyWrapper(MetaTableKey(None,None,null),Some(b)))
0:
List()
```

0层级为最外层语句；1层级为第一个子查询；2层级为第二个子查询，他包含了子查询别名以及该子查询里所有的实体表信息。

上面只是为了显示，实际上还包含了所有列的信息。这意味着，如果我要补全0层记得 project,那我只需要获取1层级的信息，可以补全b表名称或者b表对应的字段。同理类推。







