package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.JobManager

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLMLSQLJobExt(override val uid: String) extends SQLAlg with WowParams with Logging with WowLog {


  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val groupId = new MLSQLJobCollect(spark, null).getGroupId(path)
    val owner = ScriptSQLExec.contextGetOrForTest().owner

    if (!JobManager.getJobInfo.contains(groupId)) {
      logWarning(format(s"You can not kill the job $path cause it not exist any more"))
      import df.sparkSession.implicits._
      return Seq(("", s"You can not kill the job $path cause it not exist any more")).toDF("param", "description")
    }

    val isOwnerTheSame = JobManager.getJobInfo.filter(f => f._2.groupId == groupId).filter(f => f._2.owner == owner).size == 1
    if (!isOwnerTheSame) {
      logWarning(format(s"You can not kill the job $path cause you are not the owner"))
      import df.sparkSession.implicits._
      return Seq(("", s"You can not kill the job $path cause you are not the owner")).toDF("param", "description")
    }
    try{
      JobManager.killJob(spark, groupId)
    } catch {
      case e:Exception =>
    }
    import df.sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new RuntimeException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = null

  override def skipPathPrefix: Boolean = true

}
