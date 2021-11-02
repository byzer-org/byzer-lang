/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import java.util.{Date, Properties}
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Address, Authenticator, Message, PasswordAuthentication, Session, Transport}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.SQLSendMessage.MailTypeEnum.MailTypeEnum
import streaming.dsl.mmlib.algs.SQLSendMessage._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Dynamic, Extra, FormParams, KV, Select, Text}
import tech.mlsql.common.utils.log.Logging

/**
 * Created by fchen on 2018/8/22.
 */
class SQLSendMessage(override val uid: String) extends SQLAlg with Functions with BaseParams {

  val from: Param[String] = new Param[String](this, "from",
    FormParams.toJson(Text(
      name = "from",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. the sender
            | e.g. from = "test@gmail.com"
          """,
        label = "the sender",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val to: Param[String] = new Param[String](this, "to",
    FormParams.toJson(Text(
      name = "to",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. The target email addresses. Multiple addresses are separated by comma','.
            | e.g. to = "do_not_reply@gmail.com,do_not_reply2@gmail.com"
          """,
        label = "The target email addresses",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val cc: Param[String] = new Param[String](this, "cc",
    FormParams.toJson(Text(
      name = "cc",
      value = "",
      extra = Extra(
        doc =
          """
            | The email address of the CC people. Multiple addresses are separated by comma','.
            | e.g. cc = "do_not_reply@gmail.com,do_not_reply2@gmail.com"
          """,
        label = "The email address of the CC people",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val subject: Param[String] = new Param[String](this, "subject",
    FormParams.toJson(Text(
      name = "subject",
      value = "",
      extra = Extra(
        doc =
          """
            | The title of email
          """,
        label = "The title of email",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  val smtpHost: Param[String] = new Param[String](this, "smtpHost",
    FormParams.toJson(Dynamic(
      name = "smtpHost",
      extra = Extra(
        """
          | Required. Server of email
          | e.g. smtpHost = "smtp.163.com"
          |""".stripMargin,
        label = "SMTP Email Server",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "STATIC_BIND"
        )),
      subTpe = "Text",
      depends = List(":mailType==config"),
      valueProviderName = ""
    )
    ))

  val smtpPort: Param[String] = new Param[String](this, "smtpPort",
    FormParams.toJson(Dynamic(
      name = "smtpPort",
      extra = Extra(
        """
          | Server port of email
          | e.g. smtpHost = "465"
          |""".stripMargin,
        label = "SMTP Email Server Port",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "STATIC_BIND"
        )),
      subTpe = "Text",
      depends = List(":mailType==config"),
      valueProviderName = ""
    )
    ))

  val method: Param[String] = new Param[String](this, "method",
    FormParams.toJson(Select(
      name = "method",
      values = List(),
      extra = Extra(
        doc =
          """
            | Required. Way of sending
          """,
        label = "Way of sending",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("method"), Option("MAIL")))
      })
    )
    )
  )

  val mailType: Param[String] = new Param[String](this, "mailType",
    FormParams.toJson(Select(
      name = "mailType",
      values = List(),
      extra = Extra(
        doc =
          """
            | Required. The configuration type of the mail service. When the value of mailType is `config`,
            | the user name and password need to be configured at the sql level;
            | When the value is `local_server`, there is no need to configure the user
            | name and password, and the local sendmail service will be connected
            | to send mail.
          """,
        label = "Configuration type of mail service",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("mailType"), Option("config")))
        List(KV(Option("mailType"), Option("local_server")))
      })
    )
    )
  )
  setDefault(mailType, "config")

  val userName: Param[String] = new Param[String](this, "userName",
    FormParams.toJson(Dynamic(
      name = "userName",
      extra = Extra(
        """
          | Required. The username of the email sender.
          | e.g. username = "do_not_reply@gmail.com"
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "STATIC_BIND"
        )),
      subTpe = "Text",
      depends = List(":mailType==config"),
      valueProviderName = ""
    )
    ))

  val password: Param[String] = new Param[String](this, "password",
    FormParams.toJson(Dynamic(
      name = "password",
      extra = Extra(
        """
          | Required. The password of the email sender.
          | e.g. password = ""
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "STATIC_BIND"
        )),
      subTpe = "Text",
      depends = List(":mailType==config"),
      valueProviderName = ""
    )
    ))

  val content: Param[String] = new Param[String](this, "content",
    FormParams.toJson(Text(
      name = "content",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. The server of email
          """,
        label = "The email server",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  def this() = this(BaseParams.randomUID())

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val messageCount = df.count()
    require(messageCount <= MAX_MESSAGE_THRESHOLD, s"message count should <= $MAX_MESSAGE_THRESHOLD!")

    val to = params("to")
    val from = params.getOrElse("from", null)
    val cc = params.getOrElse("cc", null)
    val subject = params.getOrElse("subject", "no subject!")
    val method = params("method")
    require(to != null, "the parameter [to] cannot be empty!")
    val agent = new MailAgent()
    if (df.count() == 0) {
      method.toUpperCase() match {
        case "MAIL" =>
          logInfo(format(s"send content: ${$(content)} to $to"))
          agent.sendMessage(to, cc, null, from, subject, $(content), params)
        case _ =>
          throw new RuntimeException("unsupported method!")
      }
    } else {
      df.toJSON.collect().foreach(contentJson => {
        method.toUpperCase() match {
          case "MAIL" =>
            logInfo(format(s"send content: $contentJson to $to"))
            agent.sendMessage(to, cc, null, from, subject, contentJson, params)
          case _ =>
            throw new RuntimeException("unsupported method!")
        }
      })
    }
    logInfo(format("Email sent successfully!"))
    emptyDataFrame()(df)

  }

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  override def doc: Doc = Doc(HtmlDoc,
    """
      | SendMessage provides the ability to send messages externally. Currently, it only supports sending messages by mail, and supports single mail and batch mailing. The mail service provides two ways, one is to configure the email account of the mail sender through MLSQL and directly connect to the SMTP service to send mail, and the other is to connect to the local sendmail service to send mail.
      |
      |scenes to be used:
      |1. After the data is calculated and processed, a download link is generated and emailed to relevant personnel
      |2. When the amount of data is small, the data processing result can be sent directly
      |
      | Use "load modelParams.`SendMessage` as output;"
      | to check the available parameters;
      |
      | Use "load modelExample.`SendMessage` as output;"
      | get example.
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode,
    """
      | Provide code for sending mail in config mode. As follows:
      |
      | ```sql
      | set EMAIL_TITLE = "This is the title of the email";
      | set EMAIL_BODY = "body";
      | set EMAIL_TO = "do_not_reply@gmail.com";
      |
      | select "${EMAIL_BODY}" as content as data;
      |
      |- Use the configuration account method
      |run data as SendMessage.``
      |where method="mail"
      |and from = "do_not_reply@gmail.com"
      |and to = "${EMAIL_TO}"
      |and subject = "${EMAIL_TITLE}"
      |and smtpHost = "smtp.gmail.com"
      |and smtpPort="465"
      |and `userName`="do_not_reply@gmail.com"
      |and password="your Email identify code"
      |;
      |```
      |
      |- Provide code for sending mail in local mode. As follows:
      |
      |```sql
      |run data as SendMessage.``
      |where method="mail"
      | and `mailType`="local"
      | and from = "do_not_reply@gmail.com"
      | and to = "${EMAIL_TO}"
      | and subject = "${EMAIL_TITLE}"
      | ;
      | ```
      |
      | After sending successfully, the following log will be displayed:
      |
      | ```
      | Email sent successfully!
      | ```
      |""".stripMargin)
}

object SQLSendMessage {
  val MAX_MESSAGE_THRESHOLD = 10

  private[algs] object MailTypeEnum extends Enumeration {
    type MailTypeEnum = Value
    val CONFIG: Value = Value("config")
    val LOCAL: Value = Value("local")

    def checkExists(optionValues: String): Boolean = this.values.exists(_.toString == optionValues)
  }
}

class MailAgent() extends Logging with WowLog {
  // If the value is set, the parameter description can be changed to: required" -> "false"
  final val GLOBAL_USER_NAME: String = null
  final val GLOBAL_IDENTIFY_CODE: String = null

  // throws MessagingException
  def sendMessage(to: String,
                  cc: String,
                  bcc: String,
                  from: String,
                  subject: String,
                  content: String,
                  params: Map[String, String]): Unit = {
    var message: Message = null
    val mailType = params.getOrElse("mailType", "config")
    assert(SQLSendMessage.MailTypeEnum.checkExists(mailType), s"Unsupported mailType: $mailType!")
    val session = createSession(SQLSendMessage.MailTypeEnum.withName(mailType), params)
    message = new MimeMessage(session)
    if (from != null) {
      message.setFrom(new InternetAddress(from))
    }
    setToCcBccRecipients(message, to, cc, bcc)

    message.setSentDate(new Date())
    message.setSubject(subject)
    message.setText(content)
    Transport.send(message)
  }


  def createSession(mailType: MailTypeEnum, params: Map[String, String]): Session = {
    val properties = new Properties
    if (mailType == MailTypeEnum.CONFIG) {
      val smtpHost = params("smtpHost")
      val smtpPort = params.getOrElse("smtpPort", "465")
      require(smtpHost != null)
      properties.put("mail.host", smtpHost)
      properties.put("mail.localhost", smtpHost)
      properties.put("mail.smtp.host", smtpHost)
      properties.put("mail.smtp.localhost", smtpHost)
      properties.put("mail.smtp.auth", "true")
      properties.setProperty("mail.smtp.socketFactory.fallback", "false")
      properties.setProperty("mail.smtp.port", smtpPort)
      properties.setProperty("mail.smtp.socketFactory.port", smtpPort)

      val userName = params.getOrElse("userName", GLOBAL_USER_NAME)
      val password = params.getOrElse("password", GLOBAL_IDENTIFY_CODE)
      assert(userName != null, "userName can not be null!")
      assert(password != null, "password can not be null!")
      return Session.getInstance(properties, new Authenticator() {
        override protected def getPasswordAuthentication = new PasswordAuthentication(userName, password)
      })
    }
    Session.getInstance(properties, null)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients(message: Message, to: String, cc: String, bcc: String): Unit = {
    setMessageRecipients(message, to, Message.RecipientType.TO)
    if (cc != null) {
      setMessageRecipients(message, cc, Message.RecipientType.CC)
    }
    if (bcc != null) {
      setMessageRecipients(message, bcc, Message.RecipientType.BCC)
    }
  }

  // throws AddressException, MessagingException
  def setMessageRecipients(message: Message, recipients: String, recipientType: Message.RecipientType): Unit = {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipients).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0)) {
      message.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    InternetAddress.parse(address)
  }

}
