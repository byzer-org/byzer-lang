package streaming.common.nginx.parser


import scala.collection.mutable.ArrayBuffer

/**
 * Created by allwefantasy on 1/28/16.
 */
object NginxParser {
  def parse(line: String) = {
    Extractor.parse(line)
  }
}

object Extractor {
  private def isPosValid(pos: Position) = {
    pos.clzz != "-" && pos.start > -1 && pos.end > -1
  }

  def parse(line: String) = {
    val quotaExtractor = new QuotaExtractor
    val squareBracketExtractor = new SquareBracketExtractor
    val otherExtractor = new OtherExtractor
    val _truncs = new ArrayBuffer[Position]()
    for (i <- 0 until line.length) {
      var success = false
      val tt = quotaExtractor.extract(i, line)
      if (isPosValid(tt)) _truncs += tt

      if (tt.clzz != "-") success = true

      if (tt.clzz == "-") {
        val tt2 = squareBracketExtractor.extract(i, line)
        if (isPosValid(tt2)) _truncs += tt2

        if (tt2.clzz != "-") success = true

      }

      val item = otherExtractor.extract(success, i, line)
      if (isPosValid(item)) _truncs += item

    }
    _truncs.flatMap { f =>
      val item = line.substring(f.start, f.end + 1)
      if (f.clzz == "OtherExtractor") {
        item.split("\\s+")
      } else List(item)
    }.filterNot(f => f.trim.isEmpty)
  }

  def print(_truncs: ArrayBuffer[Position], line: String) = {
    _truncs.foreach { f =>
      println(line.substring(f.start, f.end + 1))
    }
  }

}

trait Extractor {
  def extract(i: Int, line: String): Position
}

class QuotaExtractor extends Extractor {
  var QUOTA_START = false

  var tempPos = -1


  def extract(i: Int, line: String): Position = {
    if (isQuotaBegin(i, line)) {
      QUOTA_START = true
      tempPos = i
      return Position(-1, -1, "QuotaExtractor")
    }

    if (isQuotaEnd(i, line)) {
      val temp = Position(tempPos, i, "QuotaExtractor")
      tempPos = -1
      QUOTA_START = false
      return temp
    }
    if (isQuotaInProcess(i, line)) {
      return Position(-1, -1, "QuotaExtractor")
    }
    return Position(-1, -1, "-")
  }

  def isQuotaBegin(i: Int, line: String) = {
    !QUOTA_START && realQuota(i, line)
  }

  def isQuotaInProcess(i: Int, line: String) = {
    QUOTA_START && tempPos < i
  }

  def isQuotaEnd(i: Int, line: String) = {
    QUOTA_START && tempPos != i && realQuota(i, line)
  }

  def realQuota(i: Int, line: String) = {
    line(i) == '"' && line(i - 1) != '\\' && i > 0
  }

}

class SquareBracketExtractor extends Extractor {
  var Square_Bracket_START = false
  var tempPos = -1

  def extract(i: Int, line: String): Position = {
    if (isSquareBracketBegin(i, line)) {
      Square_Bracket_START = true
      tempPos = i
      return Position(-1, -1, "SquareBracketExtractor")
    }

    if (isSquareBracketEnd(i, line)) {
      val temp = Position(tempPos, i, "SquareBracketExtractor")
      tempPos = -1
      Square_Bracket_START = false
      return temp
    }

    if (isSquareBracketInProcess(i, line)) {
      return Position(-1, -1, "SquareBracketExtractor")
    }

    return Position(-1, -1, "-")
  }

  def isSquareBracketBegin(i: Int, line: String) = {
    !Square_Bracket_START && realSquareBracketStart(i, line)
  }


  def isSquareBracketEnd(i: Int, line: String) = {
    Square_Bracket_START && tempPos != i && realSquareBracketEnd(i, line)
  }

  def isSquareBracketInProcess(i: Int, line: String) = {
    Square_Bracket_START && tempPos < i
  }

  def realSquareBracketStart(i: Int, line: String) = {
    line(i) == '[' && line(i - 1) != '\\' && i > 0
  }

  def realSquareBracketEnd(i: Int, line: String) = {
    line(i) == ']' && line(i - 1) != '\\' && i > 0
  }

}

class OtherExtractor {
  var OTHER_START = false
  var tempPos = -1

  def extract(preSuccess: Boolean, i: Int, line: String): Position = {
    if (!preSuccess && !OTHER_START) {
      OTHER_START = true
      tempPos = i
      return Position(-1, -1, "OtherExtractor")
    }
    if ((preSuccess && OTHER_START && i > tempPos) || (!preSuccess && OTHER_START && line.length == i + 1)) {
      val temp = Position(tempPos, i - 1, "OtherExtractor")
      OTHER_START = false
      tempPos = -1
      return temp
    }
    return Position(-1, -1, "-")
  }
}

case class Position(start: Int, end: Int, clzz: String)
