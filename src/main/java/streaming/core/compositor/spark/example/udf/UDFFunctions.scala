package streaming.core.compositor.spark.example.udf

import org.apache.spark.sql.UDFRegistration

/**
 * 7/4/16 WilliamZhu(allwefantasy@gmail.com)
 */
object UDFFunctions {
  def my_len(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("my_len", (co: String) => {
      co.length
    })
  }
}
