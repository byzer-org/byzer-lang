package tech.mlsql.lang.cmd.compile.internal.gc

import scala.collection.mutable

case class VariableTable(name:String,variables:mutable.HashMap[String,Any],types:mutable.HashMap[String,Any])

trait CodegenContext {
  def execute(exprs: List[Expression], variableTable: VariableTable): Any
}

