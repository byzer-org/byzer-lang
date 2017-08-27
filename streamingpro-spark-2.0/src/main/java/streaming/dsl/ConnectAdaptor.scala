package streaming.dsl

import _root_.streaming.dsl.parser.DSLSQLParser._
import org.apache.spark.sql._

/**
  * Created by allwefantasy on 27/8/2017.
  */
class ConnectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    val sparkSession = scriptSQLExecListener.sparkSession
    var option = Map[String, String]()
    val reader = scriptSQLExecListener.sparkSession.read
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          reader.format(s.getText)

        case s: ExpressionContext =>
          option += (s.identifier().getText -> s.STRING().getText)
        case s: BooleanExpressionContext =>
          option += (s.expression().identifier().getText -> s.expression().STRING().getText)
        case s: TableNameContext =>

      }
    }
  }
}
