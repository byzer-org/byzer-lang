package com.intigua.antlr4.autosuggest

import org.antlr.v4.runtime.misc.Interval
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.statement.LexerUtils
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestContextTest extends BaseTest {
  test("parse") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1 as table2;
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)

  }
  test("parse partial") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)
    println(context.statements)

  }

  test("relative pos convert") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        | select * from table1
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)

    assert(context.statements.size == 2)
    // select * f[cursor]rom table1
    val tokenPos = LexerUtils.toTokenPos(wow, 5, 11)
    assert(tokenPos == TokenPos(9, TokenPosType.CURRENT, 1))
    assert(context.toRelativePos(tokenPos)._1 == TokenPos(2, TokenPosType.CURRENT, 1))
  }

  test("keyword") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | loa
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)
    val tokenPos = LexerUtils.toTokenPos(wow, 3, 4)
    assert(tokenPos == TokenPos(0, TokenPosType.CURRENT, 3))
    assert(context.suggest(tokenPos)(0) == "load")
  }

  test("spark sql") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select no_result_type, keywords, search_num, rank
        |from(
        |  select no_result_type, keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
        |  from(
        |    select no_result_type, keywords, sum(search_num) AS search_num
        |    from drugs_bad_case_di
        |    where hp_stat_date >= date_sub(current_date,30)
        |    and action_dt >= date_sub(current_date,30)
        |    and action_type = 'search'
        |    and length(keywords) > 1
        |    and (split(av, '\\.')[0] >= 11 OR (split(av, '\\.')[0] = 10 AND split(av, '\\.')[1] = 9))
        |    --and no_result_type = 'indication'
        |    group by no_result_type, keywords
        |  )a
        |)a
        |where rank <= 100
        |as drugs_keywords_rank;
        |""".stripMargin).tokens.asScala.toList
    //convert back
    val start = wow.head.getStartIndex
    val stop = wow.last.getStopIndex

    val input = wow.head.getTokenSource.asInstanceOf[DSLSQLLexer]._input
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)
    println(originalText)
    val wow2 = context.rawSQLLexer.tokenizeNonDefaultChannel(originalText).tokens.asScala.toList
    wow2.foreach(println(_))


  }
}


