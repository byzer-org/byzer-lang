package streaming.session

import java.sql.SQLException

/**
  * Created by allwefantasy on 3/6/2018.
  */
class MLSQLException(reason: String, sqlState: String, vendorCode: Int, cause: Throwable) extends SQLException(reason, sqlState, vendorCode, cause) {
  def this(reason: String, sqlState: String, cause: Throwable) = this(reason, sqlState, 0, cause)


  def this(reason: String, sqlState: String, vendorCode: Int) =
    this(reason, sqlState, vendorCode, null)

  def this(reason: String, cause: Throwable) = this(reason, null, 0, cause)

  def this(reason: String, sqlState: String) = this(reason, sqlState, vendorCode = 0)

  def this(reason: String) = this(reason, sqlState = null, vendorCode = 0)

  def this(cause: Throwable) = this(reason = null, cause)
}
