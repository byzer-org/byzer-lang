package streaming.session

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf}
import streaming.log.Logging

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
  //  private[this] lazy val sparkSessionWithUGI = new SparkSessionWithUGI(sessionUGI, conf)
  //
  //  private[this] def acquire(userAccess: Boolean): Unit = {
  //    if (userAccess) {
  //      lastAccessTime = System.currentTimeMillis
  //    }
  //  }
  //
  //  private[this] def release(userAccess: Boolean): Unit = {
  //    if (userAccess) {
  //      lastAccessTime = System.currentTimeMillis
  //    }
  ////    if (opHandleSet.isEmpty) {
  //    //      lastIdleTime = System.currentTimeMillis
  //    //    } else {
  //    //      lastIdleTime = 0
  //    //    }
  //  }

}
