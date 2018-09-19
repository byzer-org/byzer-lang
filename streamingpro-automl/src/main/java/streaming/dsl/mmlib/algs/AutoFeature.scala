package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, Row}
import com.salesforce.op._
import com.salesforce.op.features.{FeatureSparkTypes, _}
import com.salesforce.op.features.types._


/**
  * Created by allwefantasy on 17/9/2018.
  */
class AutoFeature {
  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val label = params.getOrElse("labelCol", "label")
    val sanityCheck = params.getOrElse("sanityCheck", "true").toBoolean
    val nonNullable = params.getOrElse("nonNullable", "").split(",").filterNot(_.isEmpty).toSet
    val (responseFeature, features) = FeatureBuilder.fromDataFrame[RealNN](df, label, nonNullable)
    val autoFeatures = features.transmogrify()
    val finalFeatures = if (sanityCheck) responseFeature.sanityCheck(autoFeatures) else autoFeatures
    val workflow = new WowOpWorkflow()
      .setResultFeatures(responseFeature, finalFeatures).setInputDataset[Row](df)
    val fittedWorkflow = workflow.trainFeatureModel()
    fittedWorkflow.save(path, overwrite = true)
    fittedWorkflow.computeDataUpTo(finalFeatures)
  }
}


