package streaming.form

import streaming.db.TSparkJobParameter

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 12/7/2017.
  */


object AppInstallParameterActionType extends Enumeration {
  type AppInstallParameterActionType = Value
  val NODES = Value("nodes")
  val NORMAL = Value("normal")
  val DEPENDENCY = Value("dependency")
  val PASSWORD = Value("password")
  val MasterHost = Value("masterHost")
  val SELECT = Value("select")
}

object FormType extends Enumeration {
  type FormType = Value
  val SELECT = Value("select")
  val NORMAL = Value("normal")
  val CHECKBOX = Value("checkbox")
}

object HtmlHelper {
  def link(url: String, name: String, style: String = "", target: String = "_blank") = {
    s""" <a href="${url}" class="${style}">${name}</a>  """
  }

  def button(name: String, style: String) = {
    s"""<button class="btn ${style} btn-lg">${name}</button>"""
  }
}








