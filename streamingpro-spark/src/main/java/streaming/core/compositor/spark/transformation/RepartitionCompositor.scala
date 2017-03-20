package streaming.core.compositor.spark.transformation

import java.util

import org.apache.spark.sql.DataFrame

/**
 * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RepartitionCompositor[T] extends BaseDFCompositor[T] {

  def num = {
    config[Int]("num", _configParams)
  }


  def createDF(params: util.Map[Any, Any], df: DataFrame) = {
    val _num = num.getOrElse(1)
    df.repartition(_num)
  }

}
