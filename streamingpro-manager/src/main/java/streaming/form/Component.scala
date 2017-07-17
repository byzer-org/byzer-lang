package streaming.form
import streaming.bean.Parameter

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

object FormHelper {

  def formatFormItem(item: Parameter): Parameter = {
    FormType.withName(item.formType) match {
      case FormType.SELECT =>
        val options = item.value.split(",").map { f =>
          if (f.contains(":")) {
            val Array(a, b) = f.split(":")
            s"""<option value="${b}">${a}</option>"""
          } else {
            s"""<option value="${f}">${f}</option>"""
          }

        }.mkString("")
        item.copy(value = s"""<select class="selectpicker" name="${item.name}">${options}</select>""")

      case FormType.NORMAL =>
        item.copy(value = s"""<input type="text" name="${item.name}" value="${item.value}"/>""")

      case FormType.CHECKBOX =>
        val options = item.value.split(",").map { f =>
          val Array(a, b) = if (f.contains(":")) f.split(":") else Array(f, f)
          s"""<li class="list-group-item">${a}
              <div class="material-switch pull-right">
              <input id="${b}" name="${item.name}" value="${b}" type="checkbox"/>
              <label for="${b}" class="label-warning"></label>
            </div>
          </li>"""
        }.mkString("")
        val startHtml =
          s"""
             |         <div class="row">
             |          <div class="col-xs-12">
             |              <ul class="list-group">
           """.stripMargin
        val endHtml =
          s"""
             |            </div>
             |    </div>
           """.stripMargin
        item.copy(value = startHtml + options + endHtml)
    }

  }
}






