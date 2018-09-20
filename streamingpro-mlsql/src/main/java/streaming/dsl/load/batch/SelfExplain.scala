package streaming.dsl.load.batch

import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.MLMapping

/**
  * Created by allwefantasy on 15/9/2018.
  */
trait SelfExplain extends Serializable {
  def isMatch: Boolean

  def explain: DataFrame
}

object AutoWorkflowSelfExplain {
  def apply(format: String, path: String, option: Map[String, String], sparkSession: SparkSession): AutoWorkflowSelfExplain = new AutoWorkflowSelfExplain(format, path, option)(sparkSession)
}


class AutoWorkflowSelfExplain(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) {
  private var table: DataFrame = null

  def isMatch = {
    new Then(this, Set("workflow", "workflowList", "workflowParams", "workflowExample").contains(format))
  }

  def explain() = {
    table = List(
      new AutoWorkflowStartup(format, path, option)(sparkSession),
      new AutoWorkflowList(format, path, option)(sparkSession)
    ).
      filter(explainer => explainer.isMatch).
      head.
      explain
  }

  class Then(parent: AutoWorkflowSelfExplain, isMatch: Boolean) {
    def thenDo() = {
      if (isMatch) {
        parent.explain()
      }
      new OrElse(parent, isMatch)
    }

  }

  class OrElse(parent: AutoWorkflowSelfExplain, isMatch: Boolean) {
    def orElse(f: () => DataFrame) = {
      if (!isMatch) {
        parent.table = f()
      }
      parent
    }

  }

  def get() = {
    table
  }
}

class AutoWorkflowStartup(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    format == "workflow" && path.isEmpty
  }

  override def explain: DataFrame = {

    val rows = sparkSession.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("command:list ", "load workflow.`list` as output;"))
    ), 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "desc", dataType = StringType),
        StructField(name = "command", dataType = StringType)
      )))
  }
}


class AutoWorkflowList(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "workflow" && path == "list") || (format == "workflowList")
  }

  override def explain: DataFrame = {
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    val items = ReflectHelper.method(instance, "listWorkflows").asInstanceOf[Seq[String]].map(f => Row.fromSeq(Seq(f)))
    val rows = sparkSession.sparkContext.parallelize(items, 1)
    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType)
      )))
  }
}

object ModelSelfExplain {
  def apply(format: String, path: String, option: Map[String, String], sparkSession: SparkSession): ModelSelfExplain = new ModelSelfExplain(format, path, option)(sparkSession)
}

class ModelSelfExplain(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) {

  private var table: DataFrame = null


  def isMatch = {
    new Then(this, Set("model", "modelList", "modelParams", "modelExample", "modelExplain").contains(format))
  }

  def explain() = {
    table = List(
      new ModelStartup(format, path, option)(sparkSession),
      new ModelList(format, path, option)(sparkSession),
      new ModelParams(format, path, option)(sparkSession),
      new ModelExample(format, path, option)(sparkSession),
      new ModelExplain(format, path, option)(sparkSession)
    ).
      filter(explainer => explainer.isMatch).
      head.
      explain
  }

  class Then(parent: ModelSelfExplain, isMatch: Boolean) {
    def thenDo() = {
      if (isMatch) {
        parent.explain()
      }
      new OrElse(parent, isMatch)
    }

  }

  class OrElse(parent: ModelSelfExplain, isMatch: Boolean) {
    def orElse(f: () => DataFrame) = {
      if (!isMatch) {
        parent.table = f()
      }
      parent
    }

  }

  def get() = {
    table
  }

}

class ModelStartup(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    format == "model" && path.isEmpty
  }

  override def explain: DataFrame = {

    val rows = sparkSession.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("How to use algorithm or data process module in MLSQL", "('train'|'TRAIN') tableName 'as' format '.' path 'where'? expression? booleanExpression*  \n check document: https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-grammar.md#train")),
      Row.fromSeq(Seq("List available algorithm or data process module", "load model.`list` as output;")),
      Row.fromSeq(Seq("Explain params of specific algorithm or data process modules", """load model.`params` where alg="RandomForest" as output; """))
    ), 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "desc", dataType = StringType),
        StructField(name = "command", dataType = StringType)
      )))
  }
}

class ModelList(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "list") || (format == "modelList")
  }

  override def explain: DataFrame = {

    val rows = sparkSession.sparkContext.parallelize(MLMapping.mapping.keys.toSeq.sorted, 1)
    sparkSession.createDataFrame(rows.map { algName =>
      val sqlAlg = MLMapping.findAlg(algName)
      Row.fromSeq(Seq(algName, sqlAlg.modelType.humanFriendlyName,
        sqlAlg.coreCompatibility.map(f => f.coreVersion).mkString(","),
        sqlAlg.doc.doc, sqlAlg.doc.docType.docType
      )
      )
    },
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "algType", dataType = StringType),
        StructField(name = "sparkCompatibility", dataType = StringType),
        StructField(name = "doc", dataType = StringType),
        StructField(name = "docType", dataType = StringType)
      )))
  }
}

class ModelParams(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "params") ||
      (format == "modelParams")
  }

  private def getAlg = {
    if (format == "model" && path == "params")
      option("alg")
    else {
      path
    }
  }


  override def explain: DataFrame = {
    MLMapping.findAlg(getAlg).explainParams(sparkSession)
  }
}


class ModelExample(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "example") ||
      (format == "modelExample")
  }

  private def getAlg = {
    if (format == "model" && path == "example")
      option("alg")
    else {
      path
    }
  }


  override def explain: DataFrame = {
    val alg = MLMapping.findAlg(getAlg)
    alg.codeExample

    val rows = sparkSession.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("name", getAlg)),
      Row.fromSeq(Seq("codeExample", alg.codeExample.code)),
      Row.fromSeq(Seq("codeType", alg.codeExample.codeType.codeType)),
      Row.fromSeq(Seq("doc", alg.doc.doc)),
      Row.fromSeq(Seq("docType", alg.doc.docType.docType))
    ), 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = StringType)
      )))


  }
}

class ModelExplain(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "explain") ||
      (format == "modelExplain")
  }

  private def getAlg = {
    if ((format == "model" && path == "explain") || format == "modelExplain")
      option("alg")
    else {
      path
    }
  }

  private def getPath = {
    if ((format == "model" && path == "explain"))
      option("path")
    else {
      path
    }
  }


  override def explain: DataFrame = {
    val alg = MLMapping.findAlg(getAlg)
    alg.explainModel(sparkSession, getPath, option)
  }
}
