package streaming.dsl.mmlib.algs

import java.util
import java.util.{Date, Properties}

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Message, Session, Transport}
import net.sf.json.JSONObject
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.common.http.{HttpClientUtil, HttpConstants, OpenApiUtil}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLSendMessage._
import streaming.log.Logging

/**
 * Created by fchen on 2018/8/22.
 */
class SQLSendMessage extends SQLAlg with Logging {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val messageCount = df.count()
    require(messageCount <= MAX_MESSAGE_THRESHOLD, s"message count should <= ${MAX_MESSAGE_THRESHOLD}!")

    val to = params("to")
    val method = params("method")
    require(to != null, "to is null!")
    method.toUpperCase match {
      case "MAIL" =>
        val from = params.getOrElse("from", null)
        val subject = params.getOrElse("subject", "no subject!")
        val smtpHost = params("smtpHost")
        require(smtpHost != null)
        val agent = new MailAgent(smtpHost)
        df.toJSON.collect().foreach(contentJson => {
          logInfo(s"send mail: ${contentJson} to ${to}")
          agent.sendMessage(to, null, null, from, subject, contentJson)
        })
      case "WECHAT_WARN" =>
        val apiUrl = params("apiUrl")
        val appId = params("appId")
        val appSignKey = params("appSignKey")
        require(apiUrl != null)
        require(appId != null)
        require(appSignKey != null)
        val paramsMap = new util.TreeMap[String, Object]
        paramsMap.put("email", to)
        paramsMap.put("appId", appId)
        df.toJSON.collect().foreach(contentJson => {
          paramsMap.put("timestamp", OpenApiUtil.getTimestamp)
          paramsMap.put("nonce", OpenApiUtil.getNonceStr)
          paramsMap.put("message", contentJson)
          paramsMap.put("sign", OpenApiUtil.createSHA1Sign(paramsMap, appSignKey))
          logInfo(s"send wechat warn: ${contentJson} to ${to}")
          HttpClientUtil.doPostBody(apiUrl, JSONObject.fromObject(paramsMap).toString(), HttpConstants.HttpClientConfig.DEFAULT)
        })
      case _ =>
        throw new RuntimeException("unspport method!")
    }

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }
}

object SQLSendMessage {
  val MAX_MESSAGE_THRESHOLD = 10
}

class MailAgent(smtpHost: String) {

  // throws MessagingException
  def sendMessage(to: String,
                  cc: String,
                  bcc: String,
                  from: String,
                  subject: String,
                  content: String) {
    var message: Message = null

    message = createMessage
    if (from != null) {
      message.setFrom(new InternetAddress(from))
    }
    setToCcBccRecipients(message, to, cc, bcc)

    message.setSentDate(new Date())
    message.setSubject(subject)
    message.setText(content)

    Transport.send(message)
  }

  def createMessage: Message = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    val session = Session.getDefaultInstance(properties, null)
    return new MimeMessage(session)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients(message: Message, to: String, cc: String, bcc: String) {
    setMessageRecipients(message, to, Message.RecipientType.TO)
    if (cc != null) {
      setMessageRecipients(message, cc, Message.RecipientType.CC)
    }
    if (bcc != null) {
      setMessageRecipients(message, bcc, Message.RecipientType.BCC)
    }
  }

  // throws AddressException, MessagingException
  def setMessageRecipients(message: Message, recipient: String, recipientType: Message.RecipientType) {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0)) {
      message.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    return InternetAddress.parse(address)
  }

}
