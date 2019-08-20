package tech.mlsql.ets

import org.apache.hadoop.util.ToolRunner
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.hdfs.WowFsShell

/**
  * 2019-05-07 WilliamZhu(allwefantasy@gmail.com)
  */
class HDFSCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val conf = df.sparkSession.sessionState.newHadoopConf()
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    conf.setQuietMode(false)
    var output = ""
    val fsShell = new WowFsShell(conf, path)
    try {
      ToolRunner.run(fsShell, args.toArray)
      output = fsShell.getError

      if (output == null || output.isEmpty) {
        output = fsShell.getOut
      }
    }
    finally {
      fsShell.close()
    }
    import spark.implicits._
    if (args.contains("-F")) {
      val ds = spark.createDataset(output.split("\n").toSeq)
      spark.read.json(ds)
    } else {
      spark.createDataset[String](Seq(output)).toDF("fileSystem")
    }
  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }
}
