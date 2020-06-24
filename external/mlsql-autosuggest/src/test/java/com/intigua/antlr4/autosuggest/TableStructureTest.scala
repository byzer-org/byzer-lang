package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.statement.{MetaTableKeyWrapper, SelectSuggester}
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}
import tech.mlsql.common.utils.log.Logging
import scala.collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer

/**
 * 22/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TableStructureTest extends BaseTest with Logging {


  test("s1") {
    buildMetaProvider
    val sql =
      """
        |select  from (select no_result_type from db1.table1) b;
        |""".stripMargin
    val tokens = getMLSQLTokens(sql)

    val suggester = new SelectSuggester(context, tokens, TokenPos(0, TokenPosType.NEXT, 0))
    println(suggester.sqlAST)
  }

  test("s2") {
    buildMetaProvider
    val sql =
      """
        |select  from (select no_result_type from (select no_result_type from db1.table1) b left join db2.table2) c;
        |""".stripMargin
    val tokens = getMLSQLTokens(sql)

    val suggester = new SelectSuggester(context, tokens, TokenPos(0, TokenPosType.NEXT, 0))
    printAST(suggester)
  }

  def printAST(suggester: SelectSuggester) = {
    suggester.sqlAST
    logInfo(s"SQL[${suggester.tokens.map(_.getText).mkString(" ")}]")
    logInfo(s"STRUCTURE: \n")
    suggester.table_info.foreach { item =>
      logInfo(s"Level:${item._1}")
      item._2.foreach { table =>
        logInfo(s"${table._1} => ${table._2.copy(columns = List())}")
      }
    }
  }


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
  test("single select build") {

    buildMetaProvider
    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select a.k from jack.drugs_bad_case_di as a
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(3, TokenPosType.CURRENT, 1))
    val root = suggester.sqlAST
    root.visitDown(0) { case (ast, level) =>
      println(s"${ast.name(suggester.tokens)} ${ast.output(suggester.tokens)}")
    }

    assert(suggester.suggest().map(_.name) == List("keywords"))
  }

  test("subquery  build") {
    buildMetaProvider

    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select a.k from (select * from jack.drugs_bad_case_di ) a;
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(3, TokenPosType.CURRENT, 1))
    val root = suggester.sqlAST
    root.visitDown(0) { case (ast, level) =>
      println(s"${ast.name(suggester.tokens)} ${ast.output(suggester.tokens)}")
    }

    assert(suggester.suggest().map(_.name) == List("keywords"))
  }

  test("subquery  build without prefix") {
    buildMetaProvider

    lazy val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        |select k from (select * from jack.drugs_bad_case_di ) a;
        |""".stripMargin).tokens.asScala.toList

    val suggester = new SelectSuggester(context, wow, TokenPos(1, TokenPosType.CURRENT, 1))
    val root = suggester.sqlAST
    val buffer = ArrayBuffer[String]()
    root.visitDown(0) { case (ast, level) =>

      buffer += suggester._tokens.slice(ast.start, ast.stop).map(_.getText).mkString(" ")

    }
    assert(buffer(0) == "select k from ( select * from jack . drugs_bad_case_di ) a ;")
    assert(buffer(1) == "select * from jack . drugs_bad_case_di ) a")

    suggester.table_info.map {
      case (level, table) =>
        if (level == 0) {
          assert(table.map(_._1).toList == List(MetaTableKeyWrapper(MetaTableKey(None, None, null), Some("a"))))
        }
        if (level == 1) {
          val tables = table.map(_._1).toList
          assert(tables == List(MetaTableKeyWrapper(MetaTableKey(None, Some("jack"), "drugs_bad_case_di"), None)))

        }
    }

  }

}
