package com.intigua.antlr4.autosuggest

import tech.mlsql.atuosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.atuosuggest.statement.SelectSuggester
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggesterTest extends BaseTest {

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

    context.setMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey): Option[MetaTable] = None
    })


    val suggester = new SelectSuggester(context, wow, TokenPos(0, TokenPosType.NEXT, 0))
    suggester.suggest()


  }

  test("select output") {

    context.setMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey): Option[MetaTable] = {
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
    })

    lazy val wow2 = context.lexer.tokenizeNonDefaultChannel(
      """
        |select no_result_type, keywords, search_num, rank
        |from(
        |  select *,no_result_type, keywords, search_num, row_number() over (PARTITION BY no_result_type order by search_num desc) as rank
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

    val suggester = new SelectSuggester(context, wow2, TokenPos(0, TokenPosType.NEXT, 0))
    val root = suggester.sqlAST
    root.visitDown(0) { case (ast, level) =>
      println(s"${ast.name(suggester.tokens)} ${ast.output(suggester.tokens)}")
    }


  }


}
