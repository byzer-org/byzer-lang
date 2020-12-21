package tech.mlsql.tool

import tech.mlsql.dsl.adaptor.{DslTool, LoadStatement}

/**
 * 20/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LoadRewriter(loadStat: LoadStatement) extends DslTool {
  def assign(format: String, path: String, indexerType: Option[String]): String = {
    var where = ""

    val prefix = indexerType.map(item => s"${item}_").getOrElse("")

    if (!loadStat.option.isEmpty) {
      where = "where " + loadStat.option.map { kv =>
        s"`${kv._1}` = '''${kv._2}'''"
      }.mkString(" and ")
    }

    val rewriteRaw =
      s"""
         |load ${format}.`_mlsql_indexer_.${prefix}${loadStat.format}_${path}` ${where} as ${loadStat.tableName};
         |""".stripMargin

    rewriteRaw
  }
}

