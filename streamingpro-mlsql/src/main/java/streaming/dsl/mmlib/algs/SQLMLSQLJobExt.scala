package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.StreamingproJobManager
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLMLSQLJobExt(override val uid: String) extends SQLAlg with WowParams {


  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    params.get(groupId.name).map { item =>
      set(groupId, item)
      item
    }.getOrElse {
      params.get(jobName.name).map { item =>
        set(groupId, item)
        item
      }.getOrElse {
        throw new MLSQLException(s"${jobName.name} or ${groupId.name} is required")
      }
    }

    if ($(groupId) == null) {
      val groupIds = StreamingproJobManager.getJobInfo.filter(f => f._2.jobName == $(jobName)).map(f => f._1)
      groupIds.headOption match {
        case Some(_groupId) => StreamingproJobManager.killJob(_groupId)
        case None =>
      }
    } else {
      StreamingproJobManager.killJob($(groupId))
    }

    import df.sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = null

  final val groupId: Param[String] = new Param[String](this, "groupId", "")
  final val jobName: Param[String] = new Param[String](this, "jobName", "")
}
