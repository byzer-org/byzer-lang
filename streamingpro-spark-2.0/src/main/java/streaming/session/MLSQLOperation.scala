package streaming.session

import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.{Future, RejectedExecutionException}

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import streaming.log.Logging
import streaming.session.operation._
import org.apache.spark.MLSQLSparkConst._
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.catalyst.parser.ParseException

import scala.util.control.NonFatal

/**
  * Created by allwefantasy on 4/6/2018.
  */
class MLSQLOperation(session: MLSQLSession, f: () => Unit, opId: String, desc: String, operationTimeout: Int) extends Logging {
  private[this] var state: OperationState = INITIALIZED
  private[this] var lastAccessTime = System.currentTimeMillis()
  private[this] val statementId: String = if (opId == null) UUID.randomUUID().toString else opId
  private[this] var operationException: MLSQLException = _

  private[this] var backgroundHandle: Future[_] = _

  private[this] var result: Any = _

  def getResult = backgroundHandle

  private[this] def setState(newState: OperationState): Unit = {
    state.validateTransition(newState)
    this.state = newState
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def checkState(state: OperationState): Boolean = {
    this.state == state
  }

  private[this] def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  private[this] def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new MLSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def setOperationException(opEx: MLSQLException): Unit = {
    this.operationException = opEx
  }

  def run(): Unit = {
    setState(RUNNING)
    val backgroundOperation = new Runnable() {
      override def run(): Unit = {
        try {
          session.ugi.doAs(new PrivilegedExceptionAction[Unit]() {

            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: MLSQLException => setOperationException(e)
              }
            }
          })
        } catch {
          case e: Exception => setOperationException(new MLSQLException(e))
        }
      }
    }
    try {
      // This submit blocks if no background threads are available to run this operation
      val backgroundHandle =
        session.getOpManager.asyncRun(backgroundOperation)
      setBackgroundHandle(backgroundHandle)
    } catch {
      case rejected: RejectedExecutionException =>
        setState(ERROR)
        throw new MLSQLException("The background threadpool cannot accept" +
          " new task for execution, please retry the operation", rejected)
      case NonFatal(e) =>
        log.error(s"Error executing query in background", e)
        setState(ERROR)
        throw e
    }

  }


  def setBackgroundHandle(handle: Future[_]) = this.backgroundHandle = handle

  private[this] def execute(): Unit = {
    try {
      log.info(s"Running query '$desc' with $statementId")
      //setState(PENDING)
      session.sparkSession.sparkContext.setJobGroup(statementId, desc)
      f()
      setState(FINISHED)
    } catch {
      case e: MLSQLException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, exceptionString(e))
          throw e
        }
      case e: ParseException =>
        if (!isClosedOrCanceled) {
          onStatementError(
            statementId, e.withCommand(desc).getMessage, exceptionString(e))
          throw new MLSQLException(
            e.withCommand(desc).getMessage, "ParseException", 2000, e)
        }
      case e: AnalysisException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, exceptionString(e))
          throw new MLSQLException(e.getMessage, "AnalysisException", 2001, e)
        }
      case e: HiveAccessControlException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, exceptionString(e))
          throw new MLSQLException(e.getMessage, "HiveAccessControlException", 3000, e)
        }
      case e: Throwable =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, exceptionString(e))
          throw new MLSQLException(e.toString, "<unknown>", 10000, e)
        }
    } finally {
      if (statementId != null) {
        session.sparkSession.sparkContext.cancelJobGroup(statementId)
      }
    }
  }

  private[this] def onStatementError(id: String, message: String, trace: String): Unit = {
    log.error(
      s"""
         |Error executing query as ${session.getUserName},
         |$desc
         |Current operation state ${this.state},
         |$trace
       """.stripMargin)
    setState(ERROR)
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    log.debug(s"CLOSING $statementId")
    cleanup(CLOSED)
    session.sparkSession.sparkContext.clearJobGroup()
  }

  def cancel(): Unit = {
    log.info(s"Cancel '$desc' with $statementId")
    cleanup(CANCELED)
  }

  private[this] def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    if (statementId != null) {
      session.sparkSession.sparkContext.cancelJobGroup(statementId)
    }
  }

  def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }

  def getOpId = statementId

  def getDesc = desc

  def getOperationTimeout = operationTimeout

}
