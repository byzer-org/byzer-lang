package streaming.core.compositor.spark.udf.func

import org.apache.spark.sql.UDFRegistration

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 7/29/16 WilliamZhu(allwefantasy@gmail.com)
  */
object MLFunctions {
  def parse(uDFRegistration: UDFRegistration) = {
    Functions.parse(uDFRegistration)
  }

  def mkString(uDFRegistration: UDFRegistration) = {
    Functions.mkString(uDFRegistration)
  }

}

object Functions {
  def parse(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("parse", (co: String) => {
      val parseMethod = Class.forName("org.ansj.splitWord.analysis.NlpAnalysis").getMethod("parse", classOf[String])
      val tmp = parseMethod.invoke(null, co)
      val terms = tmp.getClass.getMethod("getTerms").invoke(tmp).asInstanceOf[java.util.List[Any]]
      terms.map(f => f.asInstanceOf[ {def getName: String}].getName).toArray
    })
  }

  def mkString(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("mkString", (sep: String, co: mutable.WrappedArray[String]) => {
      co.mkString(sep)
    })
  }

  def sleep(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("sleep", (sleep: Long) => {
      Thread.sleep(sleep)
      ""
    })
  }

}
