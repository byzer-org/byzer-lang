package tech.mlsql.tool

import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.apache.commons.lang3.exception.ExceptionUtils
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging

import java.nio.charset.StandardCharsets
import java.security.{InvalidAlgorithmParameterException, InvalidKeyException, NoSuchAlgorithmException}
import javax.crypto.{Cipher, NoSuchPaddingException}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

object CipherUtils extends Logging with WowLog {
  /**
   * This is a secret key
   */
  val KEY: Array[Byte] = Array[Byte](0x74, 0x68, 0x69, 0x73, 0x49, 0x73, 0x41, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4b, 0x65, 0x79)
  val IV_SPEC = new IvParameterSpec(Array[Byte](0x12, 0x34, 0x76, 0x78, 0x91.toByte, 0xAB.toByte, 0xCD.toByte, 0xFF.toByte, 0x14, 0x54, 0xE5.toByte, 0x48, 0x22.toByte, 0xCD.toByte, 0xAC.toByte, 0x1F.toByte))

  def aesEncrypt(strToEncrypt: String, key: String, iv: String): String = {
    if (strToEncrypt == null) return null
    try {
      val (_key, _iv) = getKeyAndIvWithDefault(key, iv)
      val cipher = getCipher(Cipher.ENCRYPT_MODE, _key, _iv)
      val encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)))
      encryptedString
    } catch {
      case e: Exception =>
        logDebug(format("aes_encrypt udf An error occurred!e:" + ExceptionUtils.getMessage(e)))
        null
    }
  }

  def aesDecrypt(strToDecrypt: String, key: String, iv: String): String = {
    if (strToDecrypt == null) return null
    try {
      val (_key, _iv) = getKeyAndIvWithDefault(key, iv)
      val cipher = getCipher(Cipher.DECRYPT_MODE, _key, _iv)
      new String(cipher.doFinal(Base64.decodeBase64(strToDecrypt)), StandardCharsets.UTF_8)
    } catch {
      case e: Exception =>
        logDebug(format("aes_decrypt udf An error occurred!e:" + ExceptionUtils.getMessage(e)))
        null
    }
  }

  private def getKeyAndIvWithDefault(key: String, iv: String): (Array[Byte], IvParameterSpec) = {
    var _key = KEY
    if (key != null) {
      if (key.length != 16) {
        logDebug(format("The size of BYZER_CIPHER_AES_KEY is not 128 bit, it is simplified by default!"))
        _key = simplification(key).getBytes("utf-8")
      } else {
        _key = key.getBytes("utf-8")
      }
    }
    var _iv = IV_SPEC
    if (iv != null) {
      if (iv.length != 16) {
        logDebug(format("The size of BYZER_CIPHER_AES_IV is not 128 bit, it is simplified by default!"))
        _iv = new IvParameterSpec(simplification(iv).getBytes("utf-8"))
      } else {
        _iv = new IvParameterSpec(iv.getBytes("utf-8"))
      }
    }
    (_key, _iv)
  }

  private def simplification(key: String) = md5Hex(key).substring(0, 16)

  @throws[InvalidAlgorithmParameterException]
  @throws[InvalidKeyException]
  @throws[NoSuchPaddingException]
  @throws[NoSuchAlgorithmException]
  private def getCipher(cipherMode: Int, key: Array[Byte], iv: IvParameterSpec) = {
    val cipher = Cipher.getInstance("AES/CFB/PKCS5Padding")

    val secretKey = new SecretKeySpec(key, "AES")
    cipher.init(cipherMode, secretKey, iv)
    cipher
  }

}