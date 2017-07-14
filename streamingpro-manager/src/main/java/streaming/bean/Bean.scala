package streaming.bean

import net.sf.json.{JSONArray, JSONObject}
import streaming.db.TSparkJar
import scala.collection.JavaConversions._

import scala.io.Source

/**
  * Created by allwefantasy on 14/7/2017.
  */
object DeployParameterService {
  val str = Source.fromInputStream(this.getClass.getResourceAsStream("/install.json")).getLines().mkString("\n")
  val appParametersList = parseItems
  val parameterProcessorMapping = register()
  val parameterFieldMapping = register2()

  def register() = {
    Map(
      "select" -> new ComplexParameterProcessor(),
      "normal" -> new NoneAppParameterProcessor(),
      "checkbox" -> new ComplexParameterProcessor()

    )
  }

  def register2() = {
    Map(
      "spark" -> new SparkParameter(),
      "jar" -> new TSparkJarParameter()
    )
  }

  import net.liftweb.{json => SJSon}

  def parseJson[T](str: String)(implicit m: Manifest[T]) = {
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(str).extract[T]
  }

  def toStr(obj: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(obj)
  }

  def parseItems = {
    val result = new scala.collection.mutable.ArrayBuffer[AppParameters]()

    JSONArray.fromObject(str).foreach { f =>
      val aii = f.asInstanceOf[JSONObject]
      val item = AppParameters(aii.getString("clzz"), aii.getString("source"), aii.getString("registerType"), aii.getJSONArray("param").map { p =>
        val aipp = p.asInstanceOf[JSONObject]
        Parameter(
          name = aipp.getString("name"),
          parameterType = aipp.getString("parameterType"),
          app = aipp.getString("app"),
          desc = aipp.getString("desc"),
          label = aipp.getString("label"),
          priority = aipp.getInt("priority"),
          formType = aipp.getString("formType"),
          actionType = aipp.getString("actionType"),
          comment = aipp.getString("comment"),
          value = aipp.getString("value")
        )
      }.toList)
      result += item
    }
    result
  }

  def appParameters(source: String) = {
    val items = appParametersList.filter(f => f.source == source)
    if (items.size > 0) Some(items(0)) else None

  }

  def installSteps(app: String) = {
    appParameters(app) match {
      case Some(i) => i.param.toList
      case None => appParameters("_default_").get.param.toList
    }
  }

  def installStep(app: String, priority: Int) = {
    appParameters(app) match {
      case Some(i) =>
        process(i, priority)
      case None =>
        process(appParameters("_default_").get, priority)
    }
  }

  def process(appInstallItem: AppParameters, priority: Int) = {
    appInstallItem.param.filter(f => f.priority == priority).map {
      f =>
        val appParameterProcessor = parameterProcessorMapping(f.actionType)
        appParameterProcessor.process(f)
    }
  }
}

case class AppParameters(clzz: String,
                         source: String,
                         registerType: String,
                         param: List[Parameter])

case class Parameter(name: String,
                     parameterType: String,
                     app: String,
                     desc: String,
                     label: String,
                     priority: Int,
                     formType: String,
                     actionType: String,
                     comment: String,
                     value: String)

trait AppParameterProcessor {
  def process(actionType: Parameter): Parameter
}

class ComplexParameterProcessor extends AppParameterProcessor {
  override def process(actionType: Parameter): Parameter = {
    if (!actionType.value.isEmpty) return actionType
    if (actionType.app.isEmpty || actionType.app == "-") return actionType
    val v = DeployParameterService.parameterFieldMapping(actionType.app).value(actionType).mkString(",")
    actionType.copy(value = v)
  }
}

class NoneAppParameterProcessor extends AppParameterProcessor {
  override def process(item: Parameter): Parameter = {
    item
  }
}

class SparkParameter extends BuildSelectParameterValues {
  override def value(actionType: Parameter): List[String] = {
    val str = Source.fromInputStream(this.getClass.getResourceAsStream("/sparkParameter.json")).getLines().mkString("\n")
    import scala.collection.JavaConversions._
    val keyValue = JSONObject.fromObject(str).map {
      f =>
        (f._1.asInstanceOf[String], f._2.asInstanceOf[JSONArray].map(f => f.asInstanceOf[String]).toList)
    }.toMap
    keyValue.getOrElse(actionType.name, List())
  }
}

class TSparkJarParameter extends BuildSelectParameterValues {
  override def value(actionType: Parameter): List[String] = {
    val jrs = TSparkJar.list
    jrs.map(f => f.name + ":" + f.path)
  }
}

trait BuildSelectParameterValues {
  def value(actionType: Parameter): List[String]
}