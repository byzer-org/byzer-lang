package tech.mlsql.dsl.processor

import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.auth._
import streaming.dsl.parser.DSLSQLParser.SqlContext
import streaming.log.Logging
import streaming.parser.lisener.BaseParseListenerextends
import tech.mlsql.Stage
import tech.mlsql.dsl.adaptor.SetAdaptor

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-04-12 WilliamZhu(allwefantasy@gmail.com)
  */
class AuthProcessListener(val listener: ScriptSQLExecListener) extends BaseParseListenerextends with Logging {

  val ENABLE_RUNTIME_SELECT_AUTH = listener.sparkSession
    .sparkContext
    .getConf
    .getBoolean("spark.mlsql.enable.runtime.select.auth", false)

  private val _tables = MLSQLTableSet(ArrayBuffer[MLSQLTable]())

  def addTable(table: MLSQLTable) = {
    _tables.tables.asInstanceOf[ArrayBuffer[MLSQLTable]] += table
  }

  def withDBs = {
    _tables.tables.filter(f => f.db.isDefined)
  }

  def withoutDBs = {
    _tables.tables.filterNot(f => f.db.isDefined)
  }

  def tables() = _tables

  override def exitSql(ctx: SqlContext): Unit = {
    ctx.getChild(0).getText.toLowerCase() match {
      case "load" =>
        new LoadAuth(this).auth(ctx)

      case "select" =>
        if (!ENABLE_RUNTIME_SELECT_AUTH) {
          new SelectAuth(this).auth(ctx)
        }
      case "save" =>
        new SaveAuth(this).auth(ctx)

      case "connect" =>
        new ConnectAuth(this).auth(ctx)

      case "create" =>
        new CreateAuth(this).auth(ctx)
      case "insert" =>
        new InsertAuth(this).auth(ctx)
      case "drop" =>
        new DropAuth(this).auth(ctx)

      case "refresh" =>

      case "set" =>
        new SetAdaptor(listener, Stage.auth).parse(ctx)
        new SetAuth(this).auth(ctx)

      case "train" | "run" | "predict" =>
        new TrainAuth(this).auth(ctx)

      case "register" =>

      case _ =>
        logInfo(s"receive unknown grammar: [ ${ctx.getChild(0).getText.toLowerCase()} ].")

    }
  }
}
