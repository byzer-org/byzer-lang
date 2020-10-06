package tech.mlsql.lang.cmd.compile.internal.gc

trait CodegenContext {
  def execute(exprs: List[Expression], values: Map[String, Any]): Any
}

