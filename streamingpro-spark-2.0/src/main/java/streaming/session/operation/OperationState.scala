package streaming.session.operation

import streaming.session.MLSQLException

/**
  * Created by allwefantasy on 4/6/2018.
  */
trait OperationState {
  def toTOperationState(): String

  def isTerminal(): Boolean = false

  def validateTransition(newState: OperationState): Unit = ex(newState)

  protected def ex(state: OperationState): Unit = throw new MLSQLException(
    "Illegal Operation state transition " + this + " -> " + state, "ServerError", 1000)
}

case object INITIALIZED extends OperationState {
  override def toTOperationState(): String = "INITIALIZED_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case PENDING | RUNNING | CANCELED | CLOSED =>
    case _ => ex(newState)
  }
}

case object RUNNING extends OperationState {
  override def toTOperationState(): String = "RUNNING_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}

case object FINISHED extends OperationState {
  override def toTOperationState(): String = "FINISHED_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CANCELED extends OperationState {
  override def toTOperationState(): String = "CANCELED_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CLOSED extends OperationState {
  override def toTOperationState(): String = "CLOSED_STATE"

  override def isTerminal(): Boolean = true
}

case object ERROR extends OperationState {
  override def toTOperationState(): String = "ERROR_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object UNKNOWN extends OperationState {
  override def toTOperationState(): String = "UKNOWN_STATE"
}

case object PENDING extends OperationState {
  override def toTOperationState(): String = "PENDING_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case RUNNING | FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}