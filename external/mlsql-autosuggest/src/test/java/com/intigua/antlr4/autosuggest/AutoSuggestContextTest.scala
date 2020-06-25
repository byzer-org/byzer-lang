package com.intigua.antlr4.autosuggest

import org.antlr.v4.runtime.Token
import org.scalatest.BeforeAndAfterEach
import tech.mlsql.autosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.statement.{LexerUtils, SuggestItem}
import tech.mlsql.autosuggest.{DataType, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestContextTest extends BaseTest with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    // context.statements.clear()
  }

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
    printStatements(context.statements)
    assert(context.statements.size == 2)
  }

  def printStatements(items: List[List[Token]]) = {
    items.foreach { item =>
      println(item.map(_.getText).mkString(" "))
      println()
    }
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
    assert(context.suggest(3, 4) == List(SuggestItem("load", SpecialTableConst.KEY_WORD_TABLE, Map())))
  }

  test("spark sql") {
    val wow = context.rawSQLLexer.tokenizeNonDefaultChannel(
      """
        |SELECT CAST(25.65 AS int) from jack;
        |""".stripMargin).tokens.asScala.toList

    wow.foreach(item => println(s"${item.getText} ${item.getType}"))
  }

  test("load/select 4/10 select ke[cursor] from") {
    val wow =
      """
        | -- yes
        | load hive.`jack.db` as table1;
        | select ke from (select keywords,search_num,c from table1) table2
        |""".stripMargin
    val items = context.buildFromString(wow).suggest(4, 10)
    assert(items.map(_.name) == List("keywords"))
  }

  test("load/select 4/22 select  from (select [cursor]keywords") {
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
        val key = MetaTableKey(None, None, "table1")
        val value = Option(MetaTable(
          key, List(
            MetaTableColumn("keywords", DataType.STRING, true, Map()),
            MetaTableColumn("search_num", DataType.STRING, true, Map()),
            MetaTableColumn("c", DataType.STRING, true, Map()),
            MetaTableColumn("d", DataType.STRING, true, Map())
          )
        ))
        value
      }

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = List()
    })
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`jack.db` as table1;
        | select  from (select keywords,search_num,c from table1) table2
        |""".stripMargin).tokens.asScala.toList
    val items = context.build(wow).suggest(4, 8)
    //    items.foreach(println(_))
    assert(items.map(_.name) == List("table2", "keywords", "search_num", "c"))

  }

  test("load/select table with star") {
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
        if (key.prefix == Option("hive")) {
          Option(MetaTable(key, List(
            MetaTableColumn("a", DataType.STRING, true, Map()),
            MetaTableColumn("b", DataType.STRING, true, Map()),
            MetaTableColumn("c", DataType.STRING, true, Map()),
            MetaTableColumn("d", DataType.STRING, true, Map())
          )))
        } else None
      }

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = ???
    })
    val wow = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`db.table1` as table2;
        | select * from table2 as table3;
        | select  from table3
        |""".stripMargin).tokens.asScala.toList
    val items = context.build(wow).suggest(5, 8)
    println(items)

  }

  test("load/select table with star and func") {
    context.setDebugMode(true)
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = {
        if (key.prefix == Option("hive")) {
          Option(MetaTable(key, List(
            MetaTableColumn("a", DataType.STRING, true, Map()),
            MetaTableColumn("b", DataType.STRING, true, Map()),
            MetaTableColumn("c", DataType.STRING, true, Map()),
            MetaTableColumn("d", DataType.STRING, true, Map())
          )))
        } else None
      }

      override def list(extra: Map[String, String] = Map()): List[MetaTable] = ???
    })
    val sql =
      """
        | -- yes
        | load hive.`db.table1` as table2;
        | select * from table2 as table3;
        | select sum() from table3
        |""".stripMargin
    val items = context.buildFromString(sql).suggest(5, 12)
    println(items)

  }
  test("table alias with temp table") {
    val sql =
      """
        |select a,b,c from table1 as table1;
        |select aa,bb,cc from table2 as table2;
        |select from table1 t1  left join table2 t2  on t1.a = t2.
        |""".stripMargin

    val items = context.buildFromString(sql).suggest(4, 58)
    assert(items.map(_.name) == List("aa", "bb", "cc"))

  }
}


