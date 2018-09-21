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

