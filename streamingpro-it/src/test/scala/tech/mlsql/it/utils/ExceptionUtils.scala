package tech.mlsql.it.utils

/**
 * 24/02/2022 hellozepp(lisheng.zhanglin@163.com)
 */
object ExceptionUtils {
  def getRootCause(t: Throwable): String = {
    var t1 = t
    if (t1 == null) return ""
    while (t1 != null) {
      if (t1.getCause == null) {
        var msg = t1.getMessage
        if (msg == null) msg = t1.toString
        return msg
      }
      t1 = t1.getCause
    }
    t1.getMessage
  }
}
