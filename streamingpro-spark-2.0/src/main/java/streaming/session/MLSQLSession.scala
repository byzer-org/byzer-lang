package streaming.session

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{MLSQLSparkConst, SparkConf}

import scala.collection.mutable.{HashSet => MHSet}
import streaming.log.Logging

import scala.collection.mutable.HashSet

/**
  * Created by allwefantasy on 1/6/2018.
  */
class MLSQLSession(username: String,
                   password: String,
                   conf: SparkConf,
                   ipAddress: String,
                   withImpersonation: Boolean,
                   sessionManager: SessionManager) extends Logging {

  @volatile private[this] var lastAccessTime: Long = System.currentTimeMillis()
  private[this] var lastIdleTime = 0L

  private[this] val sessionHandle: SessionIdentifier = new SessionIdentifier()

  private[this] val activeOperationSet = new MHSet[MLSQLOperation]()

  private[this] val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getCurrentUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
        if (conf.contains(MLSQLSparkConst.PRINCIPAL) && conf.contains(MLSQLSparkConst.KEYTAB)) {
          // If principal and keytab are configured, do re-login in case of token expiry.
          // Do not check keytab file existing as spark-submit has it done
          currentUser.reloginFromKeytab()
        }
        UserGroupInformation.createProxyUser(username, currentUser)
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }
  private[this] lazy val sparkSessionWithUGI = new SparkSessionWithUGI(sessionUGI, conf)

  private[this] def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private[this] def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (activeOperationSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  def sparkSession: SparkSession = this.sparkSessionWithUGI.sparkSession

  def ugi: UserGroupInformation = this.sessionUGI

  def open(sessionConf: Map[String, String]): Unit = {
    sparkSessionWithUGI.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      activeOperationSet.foreach { op => op.close() }
      activeOperationSet.clear()
    } finally {
      release(true)
      try {
        FileSystem.closeAllForUGI(sessionUGI)
      } catch {
        case ioe: IOException =>
          throw new MLSQLException("Could not clean up file-system handles for UGI: "
            + sessionUGI, ioe)
      }
    }
  }

  def cancelOp(op: MLSQLOperation): Unit = {
    acquire(true)
    try {
      op.close()
      activeOperationSet.remove(op)
    } finally {
      release(true)
    }
  }

  def execute(f: () => Unit, desc: String, operationTimeout: Int) = {
    acquire(true)
    val operation = new MLSQLOperation(this, f, desc, operationTimeout)
    try {
      operation.run()
      activeOperationSet.add(operation)
    } catch {
      case e: MLSQLException =>
        operation.close()
        throw e
    } finally {
      release(true)
    }
  }

  def sql(sql: String, desc: String, operationTimeout: Int) = {
    var result: DataFrame = null
    execute(() => {
      result = sparkSession.sql(sql)
    }, desc, operationTimeout)
    result
  }

  def getUserName = username

}
