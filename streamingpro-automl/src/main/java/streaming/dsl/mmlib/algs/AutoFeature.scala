package streaming.dsl.mmlib.algs

import com.salesforce.op.features.FeatureBuilder
import org.apache.spark.sql.DataFrame
import com.salesforce.op.features.types._
import com.salesforce.op._
import com.salesforce.op.readers._
import com.salesforce.op.features._
import com.salesforce.op.features.types._

/**
  * Created by allwefantasy on 17/9/2018.
  */
class AutoFeature {
  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
//    val label = params.getOrElse("labelCol", "label")
//    val (survived, predictors) = FeatureBuilder.fromDataFrame[RealNN](df, response = "label")
//    val featureVector = predictors.transmogrify()
//    val checkedFeatures = survived.sanityCheck(featureVector, removeBadFeatures = true)
//    checkedFeatures
      null
  }
}
