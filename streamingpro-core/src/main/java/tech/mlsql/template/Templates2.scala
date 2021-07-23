package tech.mlsql.tool

import java.io.{StringReader, StringWriter}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.Velocity
import tech.mlsql.common.utils.func.{WowFuncParser, WowSymbol}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.common.utils.shell.command.ParamsUtil

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * this class fix a bug in Templates in common-utils.
 * Templates will replace {:all} first, then evaluate the whole string, this
 * may destroy the {:all} content.
 */
object Templates2 {
  /**
   *   Templates2.evaluate(" hello {} ",Seq("jack"))
   *   Templates2.evaluate(" hello {0} {1} {0}",Seq("jack","wow"))
   */
  def evaluate(_str: String, parameters: Seq[String]) = {
    val str = _str

    def evaluateDefaultValue(defaultV: String) = {
      val funcParser = new WowFuncParser()
      val funcOpt = funcParser.parseFunc(defaultV)

      funcOpt match {
        case Some(func) =>
          funcParser.evaluate(func, (executeFunc) => {
            executeFunc.name match {
              case "uuid" => WowSymbol(UUID.randomUUID().toString.replaceAll("-", ""))
              case "next" =>
                val index = parameters.indexOf(executeFunc.params.head.name)
                if (index != -1) {
                  WowSymbol(parameters(index + 1))
                } else executeFunc.params(1)
            }
          }).name

        case None =>
          defaultV
      }

    }

    var finalCommand = ArrayBuffer[Char]()
    val len = str.length

    def fetchParam(index: Int, defaultV: String) = {
      if (index < parameters.length && index >= 0) {
        parameters(index).toCharArray
      } else {
        evaluateDefaultValue(defaultV).toCharArray()
      }
    }


    val posCount = new AtomicInteger(0)
    val curPos = new AtomicInteger(0)

    def positionReplace(i: Int): Boolean = {
      if (str(i) == '{' && i < (len - 1) && str(i + 1) == '}') {
        finalCommand ++= fetchParam(posCount.get(), "")
        curPos.set(i + 2)
        posCount.addAndGet(1)
        return true
      }
      return false
    }


    def namedPositionReplace(i: Int): Boolean = {

      if (str(i) != '{') return false

      val startPos = i
      var endPos = i


      // now , we should process with greedy until we meet '}'
      while (endPos < len - 1 && str(endPos) != '}') {
        endPos += 1
      }

      if (startPos - 1 >= 0 && str(startPos - 1) == '$') {
        return false
      }

      val shouldBeNumber = str.slice(startPos + 1, endPos).trim
      try {
        shouldBeNumber.split(":", 2) match {
          case Array(pos, defaultV) =>
            val posInt = Integer.parseInt(pos)
            finalCommand ++= fetchParam(posInt, defaultV)
          case Array(pos) =>
            val postInt = Integer.parseInt(pos)
            finalCommand ++= fetchParam(postInt, "")
        }
      } catch {
        case e: Exception =>
          return false
      }

      curPos.set(endPos + 1)
      return true
    }

    def textEvaluate = {
      (0 until len).foreach { i =>

        if (curPos.get() > i) {
        }
        else if (positionReplace(i)) {
        }
        else if (namedPositionReplace(i)) {

        } else {
          finalCommand += str(i)
        }
      }
    }

    if (parameters.headOption.isDefined && parameters.head == "_") {
      val params = new ParamsUtil(parameters.drop(1).toArray)
      finalCommand ++= namedEvaluate(str, params.getParamsMap.asScala.toMap).toCharArray
    } else {
      textEvaluate
    }

    var res = String.valueOf(finalCommand.toArray)

    if (res.contains("{:all}")) {
      res = res.replace("{:all}", JSONTool.toJsonStr(parameters))
    }
    res
  }

  def namedEvaluate(templateStr: String, root: Map[String, AnyRef]) = {
    val context: VelocityContext = new VelocityContext
    root.map { f =>
      context.put(f._1, f._2)
    }
    val w: StringWriter = new StringWriter
    Velocity.evaluate(context, w, "", new StringReader(templateStr))
    w.toString
  }
}

class TemplateString(s: String) {

  import Templates2._

  private val namedParams = scala.collection.mutable.HashMap[String, AnyRef]()
  private val positionParams = scala.collection.mutable.ArrayBuffer[String]()

  def reset = {
    namedParams.clear()
    positionParams.clear()
  }

  def addNamedParam(key: String, value: AnyRef) = {
    namedParams += (key -> value)
    this
  }

  def addPositionParam(value: String) = {
    positionParams += value
    this
  }

  def addNamedParams(params: Map[String, AnyRef]) = {
    namedParams ++= params
    this
  }

  def addPositionParam(values: Seq[String]) = {
    positionParams ++= values
    this
  }


  def render = {
    evaluate(namedEvaluate(s, namedParams.toMap), positionParams)
  }
}
