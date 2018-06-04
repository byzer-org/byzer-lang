package streaming.session

import java.util.concurrent.ConcurrentHashMap

import streaming.log.Logging

/**
  * Created by allwefantasy on 1/6/2018.
  */
class SessionManager(name: String) extends Logging {
  private[this] val identifierToSession = new ConcurrentHashMap[SessionIdentifier, MLSQLSession]
}
