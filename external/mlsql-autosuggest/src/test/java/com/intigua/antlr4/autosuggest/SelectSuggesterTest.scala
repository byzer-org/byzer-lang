package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.statement.SelectSuggester
import tech.mlsql.autosuggest.{DataType, MLSQLSQLFunction, TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggesterTest extends BaseTest {

  def buildMetaProvider = {
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
        Option(MetaTable(key, List(
          MetaTableColumn("no_result_type", null, true, Map()),
          MetaTableColumn("keywords", null, true, Map()),
          MetaTableColumn("search_num", null, true, Map()),
          MetaTableColumn("hp_stat_date", null, true, Map()),
          MetaTableColumn("action_dt", null, true, Map()),
          MetaTableColumn("action_type", null, true, Map()),
          MetaTableColumn("av", null, true, Map())
        )))

      }

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = List()
    })


  }

  lazy val wow = context.lexer.tokenizeNonDefaultChannel(
    """
      |select no_result_type, keywords, search_num, rank
      |from(
      |  select no_result_type, keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
      |  from(
      |    select no_result_type, keywords, sum(search_num) AS search_num
      |    from jack.drugs_bad_case_di,jack.abc jack
      |    where hp_stat_date >= date_sub(current_date,30)
      |    and action_dt >= date_sub(current_date,30)
      |    and action_type = 'search'
      |    and length(keywords) > 1
      |    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
      |    --and no_result_type = 'indication'
      |    group by no_result_type, keywords
      |  )a
      |)b
      |where rank <=
      |""".stripMargin).tokens.asScala.toList

  test("select") {

    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = None

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = List()
    })


    val suggester = new SelectSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0))
    suggester.suggest()


  }

  test("project: complex attribute suggest") {

    buildMetaProvider

    lazy val wow2 = context.lexer.tokenizeNonDefaultChannel(
      """
        |select key no_result_type, keywords, search_num, rank
        |from(
        |  select  keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
        |  from(
        |    select *,no_result_type, keywords, sum(search_num) AS search_num
        |    from jack.drugs_bad_case_di,jack.abc jack
        |    where hp_stat_date >= date_sub(current_date,30)
        |    and action_dt >= date_sub(current_date,30)
        |    and action_type = 'search'
        |    and length(keywords) > 1
        |    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
        |    --and no_result_type = 'indication'
        |    group by no_result_type, keywords
        |  )a
        |)b
        |where rank <=
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow2, TokenPos(1, TokenPosType.CURRENT, 2))
    assert(suggester.suggest().map(_.name) == List("keywords"))

  }

  test("project: second level select ") {

    buildMetaProvider

    lazy val wow2 = context.lexer.tokenizeNonDefaultChannel(
      """
        |select key no_result_type, keywords, search_num, rank
        |from(
        |  select sea keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
        |  from(
        |    select *,no_result_type, keywords, sum(search_num) AS search_num
        |    from jack.drugs_bad_case_di,jack.abc jack
        |    where hp_stat_date >= date_sub(current_date,30)
        |    and action_dt >= date_sub(current_date,30)
        |    and action_type = 'search'
        |    and length(keywords) > 1
        |    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
        |    --and no_result_type = 'indication'
        |    group by no_result_type, keywords
        |  )a
        |)b
        |where rank <=
        |""".stripMargin).tokens.asScala.toList

    //    wow2.zipWithIndex.foreach{case (token,index)=>
    //    println(s"${index} $token")}
    val suggester = new SelectSuggester(context, wow2, TokenPos(12, TokenPosType.CURRENT, 3))
    assert(suggester.suggest().distinct.map(_.name) == List("search_num"))

  }

  test("project: single query with alias table name") {

    buildMetaProvider
    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select a.k from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(3, TokenPosType.CURRENT, 1))

    assert(suggester.suggest().map(_.name) == List(("keywords")))
  }

  test("project: table or attribute") {

    buildMetaProvider
    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select  from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0))

    assert(suggester.suggest().map(_.name) == List(("a"),
      ("no_result_type"),
      ("keywords"),
      ("search_num"),
      ("hp_stat_date"),
      ("action_dt"),
      ("action_type"),
      ("av")))
  }

  test("project: complex table attribute ") {

    buildMetaProvider

    lazy val wow2 = context.lexer.tokenizeNonDefaultChannel(
      """
        |select  no_result_type, keywords, search_num, rank
        |from(
        |  select  keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
        |  from(
        |    select *,no_result_type, keywords, sum(search_num) AS search_num
        |    from jack.drugs_bad_case_di,jack.abc jack
        |    where hp_stat_date >= date_sub(current_date,30)
        |    and action_dt >= date_sub(current_date,30)
        |    and action_type = 'search'
        |    and length(keywords) > 1
        |    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
        |    --and no_result_type = 'indication'
        |    group by no_result_type, keywords
        |  )a
        |)b
        |where rank <=
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow2, TokenPos(0, TokenPosType.NEXT, 0))
    println(suggester.sqlAST.printAsStr(suggester.tokens, 0))
    suggester.table_info.foreach { case (level, item) =>
      println(level + ":")
      println(item.map(_._1).toList)
    }
    assert(suggester.suggest().map(_.name) == List(("b"), ("keywords"), ("search_num"), ("rank")))

  }

  test("table layer") {
    buildMetaProvider
    val sql =
      """
        |select  from (select no_result_type from db1.table1) b;
        |""".stripMargin
    val tokens = getMLSQLTokens(sql)

    val suggester = new SelectSuggester(context, tokens, TokenPos(0, TokenPosType.NEXT, 0))
    println("=======")
    println(suggester.suggest())
    assert(suggester.suggest().head.name=="b")
  }

  

  test("project: function suggester") {

    val func = MLSQLSQLFunction.apply("split").
      funcParam.
      param("str", DataType.STRING).
      param("splitter", DataType.STRING).
      func.
      returnParam(DataType.ARRAY, true, Map()).
      build
    MLSQLSQLFunction.funcMetaProvider.register(func)

    val tableKey = MetaTableKey(None, Option("jack"), "drugs_bad_case_di")

    val metas = Map(tableKey ->
      Option(MetaTable(tableKey, List(
        MetaTableColumn("no_result_type", null, true, Map()),
        MetaTableColumn("keywords", null, true, Map()),
        MetaTableColumn("search_num", null, true, Map()),
        MetaTableColumn("hp_stat_date", null, true, Map()),
        MetaTableColumn("action_dt", null, true, Map()),
        MetaTableColumn("action_type", null, true, Map()),
        MetaTableColumn("av", null, true, Map())
      )))

    )
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
        metas(key)
      }

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = List()
    })


    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select spl  from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(1, TokenPosType.CURRENT, 3))
    println(suggester.suggest())
    assert(suggester.suggest().map(_.name) == List(("split")))
  }




}
