package tech.mlsql.tool

import streaming.dsl.ScriptSQLExec

/**
  * 使用env替换脚本中的变量(:变量名)形式
  * Created by songgr on 2020/09/18.
  */
object ScriptEnvDecode {

  def decode(code:String):String = {

    if (code == null || code.isEmpty) return code

    val envMap = ScriptSQLExec.context().execListener.env()

    val codes = code.split(" ")

    for (i <- 0 until codes.length if codes(i).nonEmpty ) {
      val tempCode = codes(i)
      if (tempCode.contains(":")) {
        val index = tempCode.indexOf(":")
        val key = tempCode.substring(index + 1)
        val value = envMap.get(key)
        if (value.isDefined) {
          codes(i) = tempCode.replaceAll(s":$key", value.get)
        }
      }
    }
    codes.mkString(" ")
  }
}
