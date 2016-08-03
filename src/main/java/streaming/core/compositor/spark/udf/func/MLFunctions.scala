package streaming.core.compositor.spark.udf.func

import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.sql.UDFRegistration

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * 7/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
object MLFunctions {
  def parse(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("parse", (co: String) => {
      NlpAnalysis.parse(co).getTerms.map(f => f.getName).toArray
    })
  }

  def mkString(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("mkString", (sep: String, co: mutable.WrappedArray[String]) => {
      co.mkString(sep)
    })
  }

}
