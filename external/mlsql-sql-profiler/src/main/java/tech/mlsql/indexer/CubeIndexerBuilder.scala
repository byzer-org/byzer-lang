package tech.mlsql.indexer

import com.alibaba.sparkcube.CubeManager
import com.alibaba.sparkcube.execution.api.{ActionResponse, JsonParserUtil}
import com.alibaba.sparkcube.optimizer.CacheIdentifier
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{CubeSharedState, DataFrame, SaveMode, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.WowParams

/**
 * 25/1/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class CubeIndexerBuilder(override val uid: String) extends SQLAlg with WowParams {
  def this() = this(WowParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val session = df.sparkSession
    import session.implicits._
    params.getOrElse(action.name, "") match {
      case "create" =>
        try {
          val flag = CubeSharedState.get(session).cubeManager.createCache(ScriptSQLExec.context().execListener.sparkSession, params(viewName.name),
            JsonParserUtil.parseCacheFormatInfo(params(cacheInfo.name)))
          if (flag) {
            Seq(("SUCCEED", "")).toDF("param", "description")
          } else {
            Seq(("FAILED", "")).toDF("param", "description")
          }
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            Seq(("ERROR", t.getMessage)).toDF("param", "description")
        }
      case "build" =>
        try {
          val identifier = CacheIdentifier(params("cacheId"))
          val (saveMode, build) = JsonParserUtil.parseBuildInfo(params("buildInfo"))
          saveMode match {
            case SaveMode.Append =>
              CubeIndexerBuilder.cubeManager.asyncBuildCache(session, identifier, build)
            case SaveMode.Overwrite =>
              CubeIndexerBuilder.cubeManager.asyncRefreshCache(session, identifier, build)
            case _ =>
              throw new UnsupportedOperationException("not supported save mode")
          }
          Seq(("SUCCEED", "")).toDF("param", "description")
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            Seq(("ERROR", t.getMessage)).toDF("param", "description")
        }
    }


  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  final val viewName: Param[String] = new Param[String](this, "viewName", "")
  final val cacheInfo: Param[String] = new Param[String](this, "cacheInfo", "json")
  final val action: Param[String] = new Param[String](this, "action", "")

  final val cacheId: Param[String] = new Param[String](this, "cacheId", "")
  final val buildInfo: Param[String] = new Param[String](this, "buildInfo", "")
}

object CubeIndexerBuilder {
  val cubeManager = new CubeManager
}
