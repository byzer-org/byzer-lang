package tech.mlsql.udf

import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{MLSQLConf, SparkEnv}
import tech.mlsql.tool.CipherUtils

/**
 * 23/4/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Functions {
  private def sparkConf = SparkEnv.get.conf.getAll.toMap

  def aesDecrypt(uDFRegistration: UDFRegistration): UserDefinedFunction =
    uDFRegistration.register("aes_decrypt", (strToDecrypt: String) => {
        CipherUtils.aesDecrypt(strToDecrypt,
          sparkConf.getOrElse(MLSQLConf.BYZER_CIPHER_AES_KEY.key, null),
          sparkConf.getOrElse(MLSQLConf.BYZER_CIPHER_AES_IV.key, null)
        )
    })

  def aesEncrypt(uDFRegistration: UDFRegistration): UserDefinedFunction =
    uDFRegistration.register("aes_encrypt", (strToEncrypt: String) => {
        CipherUtils.aesEncrypt(strToEncrypt,
          sparkConf.getOrElse(MLSQLConf.BYZER_CIPHER_AES_KEY.key, null),
          sparkConf.getOrElse(MLSQLConf.BYZER_CIPHER_AES_IV.key, null)
        )
    })

}
