package tech.mlsql.plugin.load

import org.apache.spark.sql.DataFrame
import streaming.core.datasource.{DataSourceConfig, RewriteableSource, SourceInfo}
import streaming.dsl.MLSQLExecuteContext

/**
 * 11/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultLoaderPlugin extends RewriteableSource {
  override def rewrite(df: DataFrame, config: DataSourceConfig, sourceInfo: Option[SourceInfo], context: MLSQLExecuteContext): DataFrame = {
    val conf = config.config
    var table = df

    if (conf.contains("conditionExpr")) {
      table = table.filter(conf("conditionExpr"))
    }

    val withoutColumns = if (conf.contains("withColumns")) {
      table.columns.map(_.toLowerCase).toSet.diff(conf("withColumns").split(",").map(_.toLowerCase).toSet)
    } else if (conf.contains("withoutColumns")) {
      conf("withoutColumns").split(",").toSet
    } else {
      Set.empty[String]
    }


    table = table.drop(withoutColumns.toSeq: _*)
    table
  }
}
