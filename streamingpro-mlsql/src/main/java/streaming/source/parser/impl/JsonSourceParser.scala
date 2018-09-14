package streaming.source.parser.impl

import org.apache.spark.sql.{Column, Row, WowJsonToStructs}
import org.apache.spark.sql.types.DataType
import streaming.source.parser.{SourceParser, SourceSchema}

/**
  * Created by allwefantasy on 14/9/2018.
  */
class JsonSourceParser extends SourceParser {
  override def parse(column: Column, sourceSchema: SourceSchema, options: Map[String, String]): Column = {
    withExpr(new WowJsonToStructs(sourceSchema.schema, options, column.expr))
  }
}
