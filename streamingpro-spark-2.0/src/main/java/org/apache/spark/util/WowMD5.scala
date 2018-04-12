package org.apache.spark.util

/**
  * Created by allwefantasy on 7/2/2018.
  */
object WowMD5 {
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}
