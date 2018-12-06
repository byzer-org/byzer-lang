package streaming.helper.rest

import net.csdn.common.collections.WowCollections
import streaming.db.TSparkJobParameter
import streaming.form.FormType

/**
  * Created by allwefantasy on 23/7/2017.
  */
class RestHelper {

  def isEmpty(str: String) = {
    WowCollections.isEmpty(str)
  }

  def isNotEmpty(str: String) = {
    !WowCollections.isEmpty(str)
  }

  def formatFormItem(item: TSparkJobParameter): TSparkJobParameter = {
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
        item.copy(value =
          s"""<select @change="toggle_child"  parentName="${item.parentName}" class="selectpicker" name="${item.name}">${options}</select>""".stripMargin)

      case FormType.NORMAL =>
        item.copy(value =
          s"""<input v-on:click="toggle_child" parentName="${item.parentName}" type="text" name="${item.name}" value="${item.value}"/>""".stripMargin)

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
