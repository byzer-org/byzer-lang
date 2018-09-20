package streaming.dsl.mmlib.algs

import net.csdn.common.reflect.ReflectHelper
import org.apache.spark.ml.param._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib._
import streaming.log.{Logging, WowLog}


/**
  * Created by allwefantasy on 17/9/2018.
  */
class SQLAutoFeatureExt(override val uid: String) extends SQLAlg with SQlBaseFunc with WowParams with Logging with WowLog {

  def this() = this(BaseParams.randomUID())

  final val runMode: Param[String] = new Param[String](this, "runMode",
    "train|predict")
  setDefault(runMode, "train")

  final def getRunModel: String = $(runMode)


  final val labelCol: Param[String] = new Param[String](this, "labelCol",
    "label field")
  setDefault(labelCol, "label")


  final val workflowName: Param[String] = new Param[String](this, "workflowName",
    "named this training, required")

  final val sanityCheck: BooleanParam = new BooleanParam(this, "sanityCheck",
    "check features generated is relative with the label column")
  setDefault(sanityCheck, true)


  final val nonNullable: StringArrayParam = new StringArrayParam(this, "nonNullable",
    "columns should not be null")

  setDefault(nonNullable, Array[String]())


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    saveTraningParams(df.sparkSession, params, path + "/meta")
    ReflectHelper.method(instance, "train", df, path, params).asInstanceOf[DataFrame]
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val maps = getTranningParams(df.sparkSession, path + "/meta")
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    ReflectHelper.method(instance, "batchPredict", df, path, maps._1 ++ params).asInstanceOf[DataFrame]
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"Register is not support in ${getClass}")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    val rfcParams2 = this.params.map(this.explainParam).map(f => Row.fromSeq(f.split(":", 2)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2, 1), StructType(Seq(StructField("param", StringType), StructField("description", StringType))))
  }


  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val maps = getTranningParams(sparkSession, path + "/meta")
    val instance = Class.forName("streaming.dsl.mmlib.algs.AutoFeature").newInstance()
    ReflectHelper.method(instance, "explainModel", sparkSession, path, maps._1 ++ params).asInstanceOf[DataFrame]
  }

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |## AutoFeatureExt is used to auto generate feature from DataFrame.
      |
      |AutoFeatureExt only support  model generate and batch predict, this means you can not
      |deploy it as API Application which may have <100ms latency.
      |
      |To use the result of  AutoFeatureExt , you should use
      |
      |```sql
      |-- transform data to vector
      |run table1 as AutoFeatureExt.`/tmp/model2` where runMode="predict" as table2;
      |```
      |instead of
      |
      |```sql
      |-- this will throws `not support` exception
      |register AutoFeatureExt.`/tmp/model2`  as prefict;
      |```
      |
      |show exmaple:
      |
      |> load model.`example` where alg="AutoFeatureExt" as output;
      |
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |-- load data
      |load csv.`/tmp/william/titanic.csv` where header="true" as table1;
      |
      |-- auto generate model of features
      |train table1 as AutoFeatureExt.`/tmp/model2`
      |where labelCol="Survived" and workflowName="wow";
      |
      |-- transform data to vector
      |run table1 as AutoFeatureExt.`/tmp/model2` where runMode="predict" as table2;
      |
      |-- explain the model
      |
      |load modelExplain.`/tmp/model2` where alg="AutoFeatureExt" as output;
    """.stripMargin)

  override def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_2_x)
  }

  override def modelType: ModelType = AlgType

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
