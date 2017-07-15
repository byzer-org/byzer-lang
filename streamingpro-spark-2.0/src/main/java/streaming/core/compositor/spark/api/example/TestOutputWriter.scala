package streaming.core.compositor.spark.api.example

import org.apache.spark.sql.DataFrame
import streaming.core.compositor.spark.api.OutputWriter

/**
  * Created by allwefantasy on 15/7/2017.
  */
class TestOutputWriter extends OutputWriter {
  override def write(df: DataFrame): Unit = {

  }
}
