package streaming.dsl

import streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 12/1/2018.
  */
class RegisterAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var functionName = ""
    var format = ""
    var path = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: FunctionNameContext =>
          functionName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = cleanStr(s.getText)
        case _ =>
      }
    }
    val alg = AglMapping.findAlg(format)
    val model = alg.load(path)
    val udf = alg.predict(model)
    scriptSQLExecListener.sparkSession.udf.register(functionName, udf)
  }
}


