package streaming.dsl.mmlib.algs.feature

import org.apache.spark.sql.{DataFrame, functions => F}

/**
  * Created by allwefantasy on 15/5/2018.
  */
trait BaseFeatureFunctions {
  def killOutlierValue(df: DataFrame, field: String) = {

    val quantiles = df.stat.approxQuantile(field, Array(0.25, 0.5, 0.75), 0.0)
    val Q1 = quantiles(0)
    val Q3 = quantiles(2)
    val IQR = Q3 - Q1
    val lowerRange = Q1 - 1.5 * IQR
    val upperRange = Q3 + 1.5 * IQR

    val Q2 = quantiles(1)
    //df.filter(s"value < $lowerRange or value > $upperRange")
    val udf = F.udf((a: Double) => {
      if (a < lowerRange || a > upperRange) {
        Q2
      } else a
    })
    val newDF = df.withColumn(field, udf(F.col(field)))
    newDF
  }
}
