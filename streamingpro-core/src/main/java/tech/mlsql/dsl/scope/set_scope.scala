package tech.mlsql.dsl.scope

object SetScope {
  val NAME = "scope"
}

object ScopeParameter extends Enumeration {
  type ParameterScope = Value
  val REQUEST = Value("request")
  val SESSION = Value("session")
}