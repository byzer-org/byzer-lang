package streaming.dsl.mmlib.algs.processing

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, UDFRegistration}
import streaming.dsl.mmlib.algs.processing.image.ImageSchema

/**
  * Created by allwefantasy on 29/5/2018.
  */
object UDFFunctions {
  def imageVec(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_image", (a: Row) => {
      Vectors.dense(ImageSchema.toArray(a))
    })
  }
}
