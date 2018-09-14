package streaming.source.parser.impl

import org.apache.spark.sql.{Column, WowCsvToStructs}
import streaming.source.parser.{SourceParser, SourceSchema}

/**
  * Created by allwefantasy on 14/9/2018.
  */
class CsvSourceParser extends SourceParser {
  override def parse(column: Column, sourceSchema: SourceSchema, options: Map[String, String]): Column = {
    withExpr(new WowCsvToStructs(sourceSchema.schema, options, column.expr))
  }
}
