package streaming.core.compositor.spark.udf.func

import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.sql.UDFRegistration

import scala.collection.JavaConversions._

/**
 * 7/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
object AnalysisFunctions {
  def parse(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("parse", (co: String) => {
      NlpAnalysis.parse(co).getTerms.map(f => f.getName).toArray
    })
  }
}
