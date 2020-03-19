package tech.mlsql.template

import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.base.Templates

/**
 * 11/3/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLSnippetTemplateForScala {
  def get(templateName: String, parameters: Array[String]): String = {
    val templateContent = ScriptSQLExec.context().execListener.env()(templateName)
    Templates.evaluate(templateContent, parameters)
  }


}
