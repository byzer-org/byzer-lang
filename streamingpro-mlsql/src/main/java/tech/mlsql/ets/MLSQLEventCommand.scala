package tech.mlsql.ets

import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.stream.{MLSQLStreamEventName, MLSQLStreamListenerItem, MLSQLStreamManager}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-05-25 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLEventCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    /*
      run command as  StreamEventCommand.`` where
      eventName=""
      and handleHttpUrl=""
      and method="POST"
      and params.a=""
      and params.b="";
     */
    val context = ScriptSQLExec.contextGetOrForTest()

    if (MLSQLStreamManager.isStream) {
      val streamName = context.execListener.env()("streamName")
      val handleParams = params.filter(p => p._1.startsWith("params.")).map(p => (p._1.split("\\.", 2).last, p._2))

      val eventNames = params.getOrElse("eventName", s"${MLSQLStreamEventName.started.toString},${MLSQLStreamEventName.progress.toString},${MLSQLStreamEventName.terminated.toString}").
        split(",").filterNot(_.isEmpty)
     
      eventNames.foreach { eventName =>
        MLSQLStreamManager.addListener(context.owner,
          MLSQLStreamListenerItem(UUID.randomUUID().toString,context.owner, streamName, MLSQLStreamEventName.withName(eventName),
            params(MLSQLEventCommand.handleHttpUrl), params(MLSQLEventCommand.method), handleParams))
      }

    }
    emptyDataFrame()(df)
  }


  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

}

object MLSQLEventCommand {
  val handleHttpUrl = "handleHttpUrl"
  val method = "method"
  val eventName = "eventName"
}
