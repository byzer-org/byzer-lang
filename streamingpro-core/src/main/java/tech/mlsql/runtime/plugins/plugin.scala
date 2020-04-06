package tech.mlsql.runtime.plugins

case class AddPlugin(pluginName: String, path: String, pluginType: String, version: String)

case class ETRecord(pluginName: String, commandName: Option[String], etName: String, className: String, version: String)

case class DSRecord(pluginName: String, shortFormat: Option[String], fullFormat: String, version: String)

case class AppRecord(pluginName: String, className: String, params: Seq[String], version: String)


object PluginType {
  val ET = "et"
  val DS = "ds"
  val SCRIPT = "script"
  val APP = "app"
}
