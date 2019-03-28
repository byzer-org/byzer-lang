package streaming.core.udfs

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.flink.table.functions.ScalarFunction
import org.apache.log4j.Logger
class GetJsonAttr extends  ScalarFunction {

  private val LOG = Logger.getLogger(classOf[GetJsonAttr])
  def eval(jsonStr : String, attr :String): String = {
    try {
      return JSON.parseObject(jsonStr).getString(attr)
    } catch {
      case e:Exception => LOG.warn(s"$jsonStr is parse error")
    }
    null
  }
}

class DateStrTransform extends  ScalarFunction {
  private val LOG = Logger.getLogger(classOf[DateStrTransform])

  private var sourceFormat :SimpleDateFormat = null
  private var destFormat :SimpleDateFormat = null

  def eval(dateStr : String, sourceFormatStr :String, destFormatStr :String ): String = {
    try {
      if(sourceFormat == null) {
        sourceFormat = new SimpleDateFormat(sourceFormatStr)
      }
      if(sourceFormat == null) {
        destFormat = new SimpleDateFormat(destFormatStr)
      }
      return destFormat.format(sourceFormat.parse(dateStr))
    } catch {
      case e:Exception => LOG.warn(s"$dateStr is parse error")
    }
    null
  }
}

class NullOrEmpty extends ScalarFunction {
  private val LOG = Logger.getLogger(classOf[DateStrTransform])


  def eval(str :String, emptyStr :String): String = {
    var result = str
    if (str == null || str.length == 0) {
      result = emptyStr
    }
    return result
  }
}