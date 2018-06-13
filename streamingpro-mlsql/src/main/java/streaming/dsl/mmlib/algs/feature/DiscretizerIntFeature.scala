package streaming.dsl.mmlib.algs.feature

import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql._

/**
  * Created by allwefantasy on 15/5/2018.
  */
object DiscretizerIntFeature extends BaseFeatureFunctions {
  def vectorize(df: DataFrame, mappingPath: String, fields: Seq[String]) = {

    // assemble double fields
    val _features_ = "_features_"

    val assembler = new VectorAssembler()
      .setInputCols(fields.toArray)
      .setOutputCol(_features_)

    var newDF = assembler.transform(df)

    val pca = new PCA()
      .setInputCol(_features_)
      .setOutputCol("_discretizerIntFeature_")
      .setK(3)
      .fit(newDF)
    pca.write.overwrite().save(mappingPath + "/discretizerIntFeature/pca")
    newDF = pca.transform(newDF)

    (fields ++ Seq("_features_")).foreach { f =>
      newDF = newDF.drop(f)
    }
    newDF
  }
}
