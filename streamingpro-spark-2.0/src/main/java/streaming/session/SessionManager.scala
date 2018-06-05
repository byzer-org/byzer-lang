package streaming.session

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf
import streaming.log.Logging

/**
  * Created by allwefantasy on 1/6/2018.
  */
class SessionManager(val conf: SparkConf) extends Logging {

  private[this] val identifierToSession = new ConcurrentHashMap[SessionIdentifier, MLSQLSession]
  private[this] var shutdown: Boolean = false
  private[this] val opManager = new MLSQLOperationManager(60)


  def start(): Unit = {
    opManager.start()
    SparkSessionCacheManager.startCacheManager(conf)
  }

  def stop(): Unit = {
    shutdown = true
    SparkSessionCacheManager.get.stop()
  }

  def openSession(
                   username: String,
                   password: String,
                   ipAddress: String,
                   sessionConf: Map[String, String],
                   params: Map[Any, Any],
                   withImpersonation: Boolean): SessionIdentifier = {

    val session = new MLSQLSession(
      username,
      password,
      conf.clone(),
      ipAddress,
      withImpersonation,
      this, opManager
    )
    log.info(s"Opening session for $username")
    session.open(sessionConf,params: Map[Any, Any])

    val sessionIdentifier = session.getSessionIdentifier
    identifierToSession.put(sessionIdentifier, session)
    sessionIdentifier
  }

  def getSession(sessionIdentifier: SessionIdentifier): MLSQLSession = {
    val session = identifierToSession.get(sessionIdentifier)
    if (session == null) {
      throw new MLSQLException("Invalid SessionIdentifier: " + sessionIdentifier)
    }
    session
  }

  def closeSession(sessionIdentifier: SessionIdentifier) {
    val session = identifierToSession.remove(sessionIdentifier)
    if (session == null) {
      throw new MLSQLException(s"Session $sessionIdentifier does not exist!")
    }
    val sessionUser = session.getUserName
    SparkSessionCacheManager.get.decrease(sessionUser)
    session.close()
  }

  def getOpenSessionCount: Int = identifierToSession.size
}


