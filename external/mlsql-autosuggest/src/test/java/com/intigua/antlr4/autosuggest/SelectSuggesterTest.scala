package com.intigua.antlr4.autosuggest

import tech.mlsql.atuosuggest.statement.{LexerUtils, SelectSuggester, SingleStatementAST}
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggesterTest extends BaseTest {
  test("select") {
    val wow = context.lexer.tokenizeNonDefaultChannel(
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
    val suggester = new SelectSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0))
    // suggester.suggest().foreach(println(_))


    val newTokens = LexerUtils.toRawSQLTokens(context, wow)
    newTokens.foreach(println(_))
    val root = SingleStatementAST._build(suggester, newTokens, 0, newTokens.size, false)
    println(root.printAsStr(newTokens, 0))
    println(root.output(newTokens))


  }


}
