package streaming.dsl

import org.apache.spark.sql.SparkSession
import streaming.dsl.parser.DSLSQLParser.SqlContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 27/8/2017.
  */
class SelectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    val chunks = new ArrayBuffer[String]()
    (0 to ctx.getChildCount - 1).foreach { index =>
      ctx.getChild(index).getText match {
        case "." =>
          chunks(index - 1) = chunks(index - 1) + s".${ctx.getChild(index + 1)}"
        case _ =>
          if (index == 0 || ctx.getChild(index - 1).getText != ".") {
            chunks += ctx.getChild(index).getText
          }

      }
    }
    val tableName = chunks.last
    val sql = (0 to chunks.length - 3).map(index => chunks(index)).mkString(" ")
    scriptSQLExecListener.sparkSession.sql(sql).createOrReplaceTempView(tableName)
  }
}
