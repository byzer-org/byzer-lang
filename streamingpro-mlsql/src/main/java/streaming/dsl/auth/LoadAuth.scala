package streaming.dsl.auth

import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.{AuthProcessListener, DslTool}
import streaming.dsl.template.TemplateMerge


/**
  * Created by allwefantasy on 11/9/2018.
  */
class LoadAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    var format = ""
    var option = Map[String, String]()
    var path = ""
    var tableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case s: PathContext =>
          path = s.getText

        case s: TableNameContext =>
          tableName = s.getText
        case _ =>
      }
    }

    val mLSQLTable = MLSQLTable(None, Some(cleanStr(path)), TableType.from(format).get)
    authProcessListener.addTable(mLSQLTable)

    authProcessListener.addTable(MLSQLTable(None, Some(cleanStr(tableName)), TableType.TEMP))
    TableAuthResult.empty()
    //Class.forName(env.getOrElse("auth_client", "streaming.dsl.auth.meta.client.DefaultClient")).newInstance().asInstanceOf[TableAuth].auth(mLSQLTable)
  }
}
