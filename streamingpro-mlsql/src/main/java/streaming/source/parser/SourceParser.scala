package streaming.source.parser

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Column, Row}
import streaming.source.parser.impl.{CsvSourceParser, JsonSourceParser}

/**
  * Created by allwefantasy on 14/9/2018.
  */
trait SourceParser extends Serializable {
  def parse(column: Column, sourceSchema: SourceSchema, options: Map[String, String]): Column

  def withExpr(expr: Expression): Column = new Column(expr)

}

object SourceParser {
  def getSourceParser(name: String): SourceParser = {
    if (name.contains(".")) {
      Class.forName(name).newInstance().asInstanceOf[SourceParser]
    } else {
      name match {
        case "json" => new JsonSourceParser()
        case "csv" => new CsvSourceParser()
      }
    }
  }
}
