# MLSQL智能代码提示

MLSQL智能补全功能现阶段是作为MLSQL的一个插件的形式提供的。在发布第一个版本后，我们会将其独立出来，作为一个通用的SQL提示引擎来进行后续的发展。为了方便对该项目指代，我们后续使用 【MLSQL Code Intelligence】


## 当前状态
【积极开发中，还未发布稳定版本】

## 目标
【MLSQL Code Intelligence】目标分成两个，第一个是标准SQL补全：

1. SQL关键字补全
2. 表/字段属性/函数补全
3. 可二次开发自定义对接任何Schema Provider

第二个是MLSQL语法补全：

1. 支持各种数据源提示
2. 支持临时表提示
3. 支持各种ET组件参数提示以及名称提示

对于表和字段补，函数补全，相比其他一些SQL代码提示工具，该插件可根据当前已有的信息精确推断。比如：

```sql
select  no_result_type, keywords, search_num, rank
from(
  select  [CURSOR is HERE] row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
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

如果有接口提供schema信息，会自动展开*，并且获取相关层级的信息从而非常精准的进行提示。同时，如果有shcema信息，对每个字段也支持类型提示。插件提供了非常友好和简单的接口方便用户接入自己的元数据。

## 用户指南

### 部署
参考部署文档 [MLSQL部署](http://docs.mlsql.tech/zh/installation/)
该插件作为MLSQ默认插件，所以开箱即用

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

### 编程开发

首先初始化两个此法分析器：

```scala
object AutoSuggestController {
  val lexerAndParserfactory = new ReflectionLexerAndParserFactory(classOf[DSLSQLLexer], classOf[DSLSQLParser]);
  val mlsqlLexer = new LexerWrapper(lexerAndParserfactory, new DefaultToCharStream)

  val lexerAndParserfactory2 = new ReflectionLexerAndParserFactory(classOf[SqlBaseLexer], classOf[SqlBaseParser]);
  val sqlLexer = new LexerWrapper(lexerAndParserfactory2, new RawSQLToCharStream)

}
```
接着创建AutoSuggestContext,然后用此法分析器解析sql,最后传递给context,同时传递行号和列好，即可。

```scala

val sql = params("sql")
val lineNum = params("lineNum").toInt
val columnNum = params("columnNum").toInt

val context = new AutoSuggestContext(ScriptSQLExec.context().execListener.sparkSession,
  AutoSuggestController.mlsqlLexer,
  AutoSuggestController.sqlLexer)

val sqlTokens = context.lexer.tokenizeNonDefaultChannel(sql).tokens.asScala.toList

val tokenPos = LexerUtils.toTokenPos(sqlTokens, lineNum, columnNum)
JSONTool.toJsonStr(context.build(sqlTokens).suggest(tokenPos))
```


## 开发者指南

### 解析流程
【MLSQL Code Intelligence】复用了MLSQL/Spark SQL的lexer，重写了parser部分。因为代码提示有其自身特点，就是句法在书写过程中，大部分情况下都是错误的，无法使用严格的parser来进行解析。

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

### TokenMatcher工具类

在【MLSQL Code Intelligence】中，最主要的工作是做token匹配。我们提供了TokenMatcher来完成token的匹配。TokenMatcher支持前向和后向匹配。如下token序列:

```
select a , b , c from jack
```

假设我想以token index 3(b) 为起始点，前向匹配一个逗号，identify 可以使用如下语法：

```scala
val tokenMatcher = TokenMatcher(tokens,4).forward.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
```

接着你可以调用 tokenMatcher.isSuccess来判断是否匹配成功，可以调用tokenMatcher.get 获取匹配得到匹配成功后的index,通过tokenMatcher.getMatchTokens 获取匹配成功的token集合。

注意，TokenMatcher起始位置是包含的，也就是他会将起始位置的token也加入到匹配token里去。所以在上面的例子中，start 是4而不是3. 更多例子可以查看源码。

### 快速参与贡献该项目
【MLSQL Code Intelligence】 需要大量函数的定义，方便在用户使用时给予提示。下面是我实现的`split` 函数的代码：

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







