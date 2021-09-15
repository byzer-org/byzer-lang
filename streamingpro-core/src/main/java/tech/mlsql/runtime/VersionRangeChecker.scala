package tech.mlsql.runtime

import java.util.regex.Pattern

object VersionRangeChecker {

  private val rules = Seq(EqDef, GtDef, GeDef, LtDef, LeDef, GtLeDef, GtLtDef, GeLeDef, GeLtDef)

  def isVersionCompatible(versionPattern: String, version: String): Boolean = {
    var checkInstance: VersionCheck = null

    rules.foreach(rule => {
      if (checkInstance == null) {
        val versionCheck = rule.accept(versionPattern)
        checkInstance = versionCheck
      }
    })

    if (checkInstance == null) {
      throw new RuntimeException(s"Can not resolve version pattern: ${versionPattern}")
    }

    checkInstance.inScope(version)
  }

  def isComposedVersionCompatible(composeVersionPattern: String, version1: String, version2: String): Boolean = {
    var version1Pattern: String = composeVersionPattern
    var version2Pattern: String = null

    if (composeVersionPattern.indexOf("/") != -1) {
      version1Pattern = composeVersionPattern.substring(0, composeVersionPattern.indexOf("/"))
      version2Pattern = composeVersionPattern.substring(composeVersionPattern.indexOf("/") + 1)
    }

    val version1Compatible = isVersionCompatible(version1Pattern, version1)
    var version2Compatible: Boolean = true
    if (version2Pattern != null && !version2Pattern.isEmpty) {
      version2Compatible = isVersionCompatible(version2Pattern, version2)
    }

    version1Compatible && version2Compatible
  }

}


trait VersionRangeDef {

  val name: String

  // common version pattern, like 2.1.0-GA
  val p = "(\\s*\\d+([.]\\d+)+(-[a-zA-Z0-9]+)?\\s*)"

  def accept(versionDef: String): VersionCheck

}

trait SingleVersionRangeDef extends VersionRangeDef {

  val patternWithGroups: Seq[(Pattern, Int)]

  def createVersionCheck(version: String): VersionCheck

  def accept(versionDef: String): VersionCheck = {
    var check: VersionCheck = null
    patternWithGroups.foreach(patternWithGroup => {
      if (check == null) {
        val matcher = patternWithGroup._1.matcher(versionDef)
        if (matcher.matches()) {
          check = createVersionCheck(matcher.group(patternWithGroup._2).trim)
        }
      }
    })
    check
  }
}

trait DoubleVersionRangeDef extends VersionRangeDef {

  val patternWithGroups: Seq[(Pattern, Int, Int)]

  def createVersionCheck(min: String, max: String): VersionCheck

  def accept(versionDef: String): VersionCheck = {
    var check: VersionCheck = null
    patternWithGroups.foreach(patternWithGroup => {
      if (check == null) {
        val matcher = patternWithGroup._1.matcher(versionDef)
        if (matcher.matches()) {
          check = createVersionCheck(matcher.group(patternWithGroup._2).trim, matcher.group(patternWithGroup._3).trim)
        }
      }
    })
    check
  }
}



trait VersionCheck {

  val p = Pattern.compile("((\\d+([.]\\d+)+)(-[a-zA-Z0-9]+)?)")

  def inScope(version: String): Boolean

  def compareVersion(v1: String, v2: String): Int = {
    val m1 = p.matcher(v1)
    val m2 = p.matcher(v2)

    if (!m1.matches()) {
      throw new RuntimeException(s"can not resolve version: ${v1}")
    }

    if (!m2.matches()) {
      throw new RuntimeException(s"can not resolve version: ${v2}")
    }

    val v1WithTag = (m1.group(2), m1.group(4))
    val v2WithTag = (m2.group(2), m2.group(4))

    // compare number version
    var compareResult: Int = 0
    val v1nums: Array[String] = v1WithTag._1.split("\\.")
    val v2nums: Array[String] = v2WithTag._1.split("\\.")
    val minLength = if (v1nums.length > v2nums.length) v2nums.length else v1nums.length

    for (i <- 0 until minLength) {
      if (compareResult == 0) {
        val v1num: Int = Integer.parseInt(v1nums(i))
        val v2num: Int = Integer.parseInt(v2nums(i))
        if (v1num.compareTo(v2num) != 0) {
          compareResult = v1num.compareTo(v2num)
        }
      }
    }

    if (compareResult == 0 && v1nums.length != v2nums.length) {
      compareResult = if(v1nums.length > v2nums.length) 1 else -1
    }

    // compare tag, 2.1.0-SNAPSHOT, 2.1.0-dev, 2.1.-=beta < 2.1.0
    if (compareResult == 0) {
      if (v1WithTag._2 == null && v2WithTag._2 == null) {
        return 0
      }

      if (v1WithTag._2 != null && v2WithTag._2 != null) {
        return 0
      }

      if (v1WithTag._2 != null && v2WithTag._2 == null) {
        return -1
      }

      if (v1WithTag._2 == null && v2WithTag._2 != null) {
        return 1
      }

    }
    compareResult
  }

}



object EqDef extends SingleVersionRangeDef {

  override val name: String = "=="

  override val patternWithGroups: Seq[(Pattern, Int)] = Seq(
    (Pattern.compile(s"==${p}"), 1),  // e.g. ==2.1.0
    (Pattern.compile(s"=${p}"), 1)  // e.g. =2.1.0
  )

  override def createVersionCheck(version: String): VersionCheck = {
    Eq(version)
  }
}

case class Eq(value: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    this.value.equalsIgnoreCase(version)
  }
}

object GtDef extends SingleVersionRangeDef {

  override val name: String = ">"

  override val patternWithGroups: Seq[(Pattern, Int)] = Seq(
    (Pattern.compile(s"\\(${p}\\,\\s*\\)"), 1),  // e.g. (2.1.0,)
    (Pattern.compile(s">${p}"), 1)  // e.g. >2.1.0
  )

  override def createVersionCheck(version: String): VersionCheck = {
    Gt(version)
  }
}

case class Gt(min: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) > 0
  }
}



object GeDef extends SingleVersionRangeDef {

  override val name: String = ">="

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\[${p}\\,\\s*\\)"), 1),  // e.g. [2.1.0,)
    (Pattern.compile(s"${p}"), 1),  // e.g. 2.1.0
    (Pattern.compile(s">=${p}"), 1)  // e.g. >=2.1.0
  )

  override def createVersionCheck(version: String): VersionCheck = {
    Ge(version)
  }
}

case class Ge(min: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) >= 0
  }
}

object LtDef extends SingleVersionRangeDef {

  override val name: String = "<"

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\(\\s*,${p}\\)"), 1),  // e.g. (,2.1.0)
    (Pattern.compile(s"<${p}"), 1)  // e.g. <2.1.0
  )

  override def createVersionCheck(version: String): VersionCheck = {
    Lt(version)
  }
}

case class Lt(max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, max) < 0
  }
}


object LeDef extends SingleVersionRangeDef {

  override val name: String = "<="

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\(\\s*,${p}\\]"), 1),  // e.g. (,2.1.0]
    (Pattern.compile(s"<=${p}"), 1)  // e.g. <=2.1.0
  )

  override def createVersionCheck(version: String): VersionCheck = {
    Le(version)
  }
}

case class Le(max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, max) <= 0
  }
}



object GtLeDef extends DoubleVersionRangeDef {

  override val name: String = "(,]"

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\(${p}\\,${p}\\]"), 1, 4),  // e.g. (2.1.0, 2.3.4]
    (Pattern.compile(s">${p}\\,\\s*<=${p}"), 1, 4)  // e.g. >2.1.0, <= 2.3.4
  )

  override def createVersionCheck(min: String, max: String): VersionCheck = {
    GtLe(min, max)
  }
}

case class GtLe(min: String, max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) > 0 && compareVersion(version, max) <= 0
  }
}

object GtLtDef extends DoubleVersionRangeDef {

  override val name: String = "(,)"

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\(${p}\\,${p}\\)"), 1, 4), // e.g. (2.1.0, 2.3.4)
    (Pattern.compile(s">${p}\\,\\s*<${p}"), 1, 4) // e.g. >2.1.0, <2.3.4
  )

  override def createVersionCheck(min: String, max: String): VersionCheck = {
    GtLt(min, max)
  }
}

case class GtLt(min: String, max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) > 0 && compareVersion(version, max) < 0
  }
}

object GeLeDef extends DoubleVersionRangeDef {

  override val name: String = "[,]"

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\[${p}\\,${p}\\]"), 1, 4),  // e.g. [2.1.0, 2.3.4]
    (Pattern.compile(s">=${p}\\,\\s*<=${p}"), 1, 4) // e.g. >=2.1.0,<=2.3.4
  )

  override def createVersionCheck(min: String, max: String): VersionCheck = {
    GeLe(min, max)
  }
}

case class GeLe(min: String, max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) >= 0 && compareVersion(version, max) <= 0
  }
}

object GeLtDef extends DoubleVersionRangeDef {

  override val name: String = "[,)"

  override val patternWithGroups = Seq(
    (Pattern.compile(s"\\[${p}\\,${p}\\)"), 1, 4),  // e.g. [2.1.0, 2.3.4)
    (Pattern.compile(s">=${p}\\,\\s*<${p}"), 1, 4) // e.g. >=2.1.0, <2.3.4
  )

  override def createVersionCheck(min: String, max: String): VersionCheck = {
    GeLt(min, max)
  }
}

case class GeLt(min: String, max: String) extends VersionCheck {
  override def inScope(version: String): Boolean = {
    compareVersion(version, min) >= 0 && compareVersion(version, max) < 0
  }
}
