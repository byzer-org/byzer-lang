package streaming.example

import com.salesforce.op._
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.readers._
import com.salesforce.op.features._
import com.salesforce.op.features.types._
import com.salesforce.op.stages.impl.classification._
import com.salesforce.op.test.Passenger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._

/**
  * Created by allwefantasy on 12/9/2018.
  */
object AutoMLExample {
  def main(args: Array[String]): Unit = {


//    import com.salesforce.op._
//    import com.salesforce.op.readers._
//    import com.salesforce.op.features._
//    import com.salesforce.op.features.types._
//    import com.salesforce.op.stages.impl.classification._
//    import org.apache.spark.SparkConf
//    import org.apache.spark.sql.SparkSession
//
//    implicit val spark = SparkSession.builder.config(new SparkConf()).getOrCreate()
//    import spark.implicits._
//
//    // Read Titanic data as a DataFrame
//    val passengersData = DataReaders.Simple.csvCase[Passenger](path = pathToData).readDataset().toDF()
//
//    // Extract response and predictor features
//    val (survived, predictors) = FeatureBuilder.fromDataFrame[RealNN](passengersData, response = "survived")
//
//    // Automated feature engineering
//    val featureVector = predictors.transmogrify()
//
//    // Automated feature validation and selection
//    val checkedFeatures = survived.sanityCheck(featureVector, removeBadFeatures = true)
//
//    // Automated model selection
//    val (pred, raw, prob) = BinaryClassificationModelSelector().setInput(survived, checkedFeatures).getOutput()
//
//    // Setting up a TransmogrifAI workflow and training the model
//    val model = new OpWorkflow().setInputDataset(passengersData).setResultFeatures(pred).train()
//
//    println("Model summary:\n" + model.summaryPretty())

  }
}


