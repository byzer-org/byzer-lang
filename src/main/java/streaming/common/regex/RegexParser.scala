package  streaming.common.regex.parser

import scala.collection.mutable
import scala.util.matching.Regex
/**
  *  10/20/16  sunbiaobiao(1319027852@qq.com)
  */
object  RegexParser {
  def parse(line : String, patten: String, keys: Array[String]) = {
    LogParser.parse(line, patten, keys)
  }
}


object LogParser {
  var regexer: Regex = null

  def parse(line: String, patten: String, keys: Array[String]): mutable.HashMap[String, String] = {
    if (regexer == null) {
      regexer = new Regex(patten, keys: _*)
    }

    val regexFind = regexer findFirstMatchIn line

    if (!regexFind.isDefined) {
      return null
    }
    val _match = regexFind.get
    val ret = new mutable.HashMap[String, String]()

    for (key <- keys) {
        ret.put(key, _match.group(key))
    }
    ret
  }
}
