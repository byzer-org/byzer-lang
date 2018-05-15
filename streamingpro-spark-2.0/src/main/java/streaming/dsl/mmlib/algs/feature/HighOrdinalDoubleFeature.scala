package streaming.dsl.mmlib.algs.feature

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}

/**
  * Created by allwefantasy on 15/5/2018.
  */
object HighOrdinalDoubleFeature extends BaseFeatureFunctions {
  def vectorize(df: DataFrame, mappingPath: String, fields: Seq[String]) = {
    // killOutlierValue
    var newDF = df

    fields.foreach { f =>
      newDF = killOutlierValue(newDF, f)
    }

    // assemble double fields
    val assembler = new VectorAssembler()
      .setInputCols(fields.toArray)
      .setOutputCol("_features_")
    newDF = assembler.transform(df)

    // standarize
    val scaler = new StandardScaler()
      .setInputCol("_features_")
      .setOutputCol("__highOrdinalDoubleFeature__")
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(newDF)
    scalerModel.write.overwrite().save(mappingPath + "/highOrdinalDoubleFeature/standarize")

    newDF = scalerModel.transform(newDF)

    (fields ++ Seq("_features_")).foreach { f =>
      newDF = newDF.drop(f)
    }
    newDF
  }


}
