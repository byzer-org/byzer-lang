package streaming.dsl.mmlib.algs

import com.hortonworks.spark.sql.kafka08.KafkaOperator
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{ExternalCommandRunner, WowMD5}

import scala.io.Source

/**
  * Created by allwefantasy on 1/2/2018.
  */
object SQLPythonFunc {

  def getPath(params: Map[String, String]) = {
    if (params.contains("pythonDescPath") || params.contains("pythonScriptPath")) {
      Some(params.getOrElse("pythonDescPath", params.getOrElse("pythonScriptPath", "")))
    } else None
  }

  def loadUserDefinePythonScript(params: Map[String, String], spark: SparkSession) = {
    getPath(params) match {
      case Some(path) =>
        val pathChunk = path.split("/")
        val userFileName = pathChunk(pathChunk.length - 1)
        val userPythonScriptList = spark.sparkContext.textFile(path).collect().mkString("\n")
        Some(PythonScript(userFileName, userPythonScriptList, path))
      case None => None
    }
  }

  def recordUserLog(algIndex: Int, pythonScript: PythonScript, kafkaParam: Map[String, String], res: Iterator[String]) = {
    val logPrefix = algIndex + "/" + pythonScript.filePath + ":  "
    val scores = KafkaOperator.writeKafka(logPrefix, kafkaParam, res)
    val score = if (scores.size > 0) scores.head else 0d
    score
  }

  def recordUserLog(kafkaParam: Map[String, String], line: String) = {
    KafkaOperator.writeKafka("", kafkaParam, Seq(line).toIterator)
  }

  def recordUserLog(kafkaParam: Map[String, String], res: Iterator[String]) = {
    KafkaOperator.writeKafka("", kafkaParam, res)
  }

  def recordUserException(kafkaParam: Map[String, String], e: Exception) = {
    KafkaOperator.writeKafka("", kafkaParam, Seq(e.getStackTrace.map(f => f.toString).mkString("\n")).toIterator)
  }

  def findPythonScript(userPythonScript: Option[PythonScript],
                       fitParams: Map[String, String],
                       defaultScriptName: String
                      ) = {

    var tfSource = userPythonScript.map(f => f.fileContent).getOrElse("")
    var tfName = userPythonScript.map(f => f.fileName).getOrElse("")
    var alg = tfName
    var filePath = userPythonScript.map(f => f.filePath).getOrElse("")
    if (fitParams.contains("alg")) {
      alg = fitParams("alg")
      tfName = s"mlsql_${defaultScriptName}_${alg}.py"
      filePath = s"/python/${tfName}"
      tfSource = Source.fromInputStream(ExternalCommandRunner.getClass.getResourceAsStream(filePath)).
        getLines().mkString("\n")
    } else {
      require(!tfSource.isEmpty, "pythonDescPath or fitParam.0.alg is required")
    }
    PythonScript(tfName, tfSource, filePath)
  }

  def findPythonPredictScript(sparkSession: SparkSession,
                              params: Map[String, String],
                              defaultScriptName: String
                             ) = {
    val userPythonScript = loadUserDefinePythonScript(params, sparkSession)
    userPythonScript match {
      case Some(ups) => ups
      case None =>
        val userFileName = defaultScriptName
        val path = s"/python/${userFileName}"
        val sk_bayes = Source.fromInputStream(ExternalCommandRunner.getClass.getResourceAsStream(path)).
          getLines().mkString("\n")
        PythonScript(userFileName, sk_bayes, path)
    }

  }

  // --  path related (local/hdfs)

  def getLocalTempModelPath(hdfsPath: String) = {
    s"${getLocalBasePath}/models/${WowMD5.md5Hash(hdfsPath)}"
  }

  def getLocalTempDataPath(hdfsPath: String) = {
    s"${getLocalBasePath}/data/${WowMD5.md5Hash(hdfsPath)}"
  }

  def getLocalBasePath = {
    s"/tmp/__mlsql__"
  }

  def getAlgModelPath(hdfsPath: String) = {
    s"${hdfsPath}/model"
  }

  def getAlgMetalPath(hdfsPath: String) = {
    s"${hdfsPath}/meta"
  }

  def getAlgTmpPath(hdfsPath: String) = {
    s"${hdfsPath}/tmp"
  }

  // -- path related
}

case class PythonScript(fileName: String, fileContent: String, filePath: String)
