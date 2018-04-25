package streaming.common

/**
  * Created by allwefantasy on 25/4/2018.
  */
object Md5 {
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}
