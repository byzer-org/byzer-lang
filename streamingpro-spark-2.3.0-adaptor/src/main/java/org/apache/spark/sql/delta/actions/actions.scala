package org.apache.spark.sql.delta.actions

/**
  * Only for compilation
  */
sealed trait Action {
  def wrap: SingleAction

  def json: String = ???
}

object Action {
  def fromJson(json: String): Action = ???
}

case class CommitInfo()

case class AddFile() extends Action {
  override def wrap: SingleAction = ???
}

case class RemoveFile() extends Action {
  override def wrap: SingleAction = ???
}

case class SingleAction() {
}
