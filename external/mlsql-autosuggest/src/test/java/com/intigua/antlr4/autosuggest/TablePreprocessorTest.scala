package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.meta.{MetaProvider, MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.preprocess.TablePreprocessor
import tech.mlsql.autosuggest.{DataType, SpecialTableConst}

import scala.collection.JavaConverters._

/**
 * 10/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TablePreprocessorTest extends BaseTest {

  test("load/select table") {
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable] = {
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
        | select a,b,c from table2 as table3;
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)
    val processor = new TablePreprocessor(context)
    context.statements.foreach(processor.process(_))

    val targetTable = context.tempTableProvider.search(SpecialTableConst.tempTable("table2").key).get
    assert(targetTable.key == MetaTableKey(Some("hive"), Some("db"), "table1"))

    val targetTable3 = context.tempTableProvider.search(SpecialTableConst.tempTable("table3").key).get
    assert(targetTable3 == MetaTable(MetaTableKey(None, Some(SpecialTableConst.TEMP_TABLE_DB_KEY), "table3"),
      List(MetaTableColumn("a", null, true, Map()), MetaTableColumn("b", null, true, Map()), MetaTableColumn("c", null, true, Map()))))
  }

  test("load/select table with star") {
    context.setUserDefinedMetaProvider(new MetaProvider {
      override def search(key: MetaTableKey,extra: Map[String, String] = Map()): Option[MetaTable] = {
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
        |""".stripMargin).tokens.asScala.toList
    context.build(wow)
    val processor = new TablePreprocessor(context)
    context.statements.foreach(processor.process(_))

    val targetTable = context.tempTableProvider.search(SpecialTableConst.tempTable("table2").key).get
    assert(targetTable.key == MetaTableKey(Some("hive"), Some("db"), "table1"))

    val targetTable3 = context.tempTableProvider.search(SpecialTableConst.tempTable("table3").key).get
    assert(targetTable3 == MetaTable(MetaTableKey(None, Some(SpecialTableConst.TEMP_TABLE_DB_KEY), "table3"),
      List(MetaTableColumn("a", null, true, Map()),
        MetaTableColumn("b", null, true, Map()),
        MetaTableColumn("c", null, true, Map()), MetaTableColumn("d", null, true, Map()))))
  }

}
