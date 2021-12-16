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

import org.apache.commons.lang.StringUtils

import java.util.{Date, Properties}
import javax.mail.internet.{ContentType, InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart, MimeUtility}
import javax.mail.{Address, Authenticator, BodyPart, Message, Multipart, PasswordAuthentication, Session, Transport}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.DownloadRunner
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.SQLSendMessage.MailTypeEnum.MailTypeEnum
import streaming.dsl.mmlib.algs.SQLSendMessage.{MailContentTypeEnum, _}
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.log.WowLog
import tech.mlsql.common.form.{Dynamic, Extra, FormParams, KV, Select, Text}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.HDFSOperatorV2

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, InputStream, OutputStream}
import java.nio.file.Paths
import javax.activation.DataHandler
import javax.mail.util.ByteArrayDataSource
import scala.collection.mutable

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
        List(KV(Option("mailType"), Option("config")),
          KV(Option("mailType"), Option("local")))
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
            | Required. The content of email
          """,
        label = "The email content",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  val contentType: Param[String] = new Param[String](this, "contentType",
    FormParams.toJson(Select(
      name = "contentType",
      values = List(),
      extra = Extra(
        doc =
          """
            | Format used to send email Content-Type
          """,
        label = "Format used to send email Content-Type",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("contentType"), Option(MailContentTypeEnum.MIXED.toString)),
          KV(Option("contentType"), Option(MailContentTypeEnum.TEXT.toString)),
          KV(Option("contentType"), Option(MailContentTypeEnum.HTML.toString)),
          KV(Option("contentType"), Option(MailContentTypeEnum.CSV.toString)),
          KV(Option("contentType"), Option(MailContentTypeEnum.JPEG.toString)),
          KV(Option("contentType"), Option(MailContentTypeEnum.DEFAULT_ATTACHMENT.toString))
        )
      })
    )
    )
  )
  setDefault(contentType, MailContentTypeEnum.TEXT.toString)
  val attachmentContentType: Param[String] = new Param[String](this, "attachmentContentType",
    FormParams.toJson(Select(
      name = "attachmentContentType",
      values = List(),
      extra = Extra(
        doc =
          """
            | Format used to send email attachment Content-Type
          """,
        label = "Format used to send email content",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(KV(Option("attachmentContentType"), Option(MailContentTypeEnum.MIXED.toString)),
          KV(Option("attachmentContentType"), Option(MailContentTypeEnum.TEXT.toString)),
          KV(Option("attachmentContentType"), Option(MailContentTypeEnum.HTML.toString)),
          KV(Option("attachmentContentType"), Option(MailContentTypeEnum.CSV.toString)),
          KV(Option("attachmentContentType"), Option(MailContentTypeEnum.JPEG.toString)),
          KV(Option("attachmentContentType"), Option(MailContentTypeEnum.DEFAULT_ATTACHMENT.toString)))
      })
    )
    )
  )
  setDefault(attachmentContentType, MailContentTypeEnum.DEFAULT_ATTACHMENT.toString)
  val attachmentPaths: Param[String] = new Param[String](this, "attachmentPaths",
    FormParams.toJson(Text(
      name = "attachmentPaths",
      value = "",
      extra = Extra(
        doc =
          """
            | The file address of the attachment, multiple addresses are separated by','.
          """,
        label = "The email content",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  val charset: Param[String] = new Param[String](this, "charset",
    FormParams.toJson(Text(
      name = "charset",
      value = "",
      extra = Extra(
        doc =
          """
            | The charset of email content
          """,
        label = "The charset of email content",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(charset, "utf-8")
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
    val content = params.getOrElse("content", "")
    val to = params("to")
    val from = params.getOrElse("from", null)
    val cc = params.getOrElse("cc", null)
    val subject = params.getOrElse("subject", "no subject!")
    val method = params("method")
    val charset = params.getOrElse("charset", "utf-8")
    val contentType = params.getOrElse("contentType", MailContentTypeEnum.TEXT.toString)
    val attachmentContentType = params.getOrElse("attachmentContentType", MailContentTypeEnum.DEFAULT_ATTACHMENT.toString)
    val attachmentPaths = params.getOrElse("attachmentPaths", null)
    val attachmentMap = mutable.Map.empty[String, (String, String)]
    if (attachmentPaths != null) {
      attachmentPaths.split(",").foreach(path => {
        if (StringUtils.isNotBlank(path) && path.lastIndexOf(File.separatorChar) != -1) {
          val separatorIndex = path.lastIndexOf(File.separatorChar)
          val filePath = path.substring(0, separatorIndex + 1)
          val fileName = path.substring(separatorIndex + 1)
          attachmentMap.put(path, filePath -> fileName)
        } else if (StringUtils.isNotBlank(path)) {
          attachmentMap.put(path, path -> path)
        }
      })
    }

    require(to != null, "the parameter [to] cannot be empty!")
    val agent = new MailAgent()
    method.toUpperCase() match {
      case "MAIL" =>
        logInfo(format(s"send content: $content to $to"))
        agent.sendMessage(to, cc, null, from, subject, content, params,
          contentType, attachmentContentType, attachmentMap.toMap, charset)
      case _ =>
        throw new RuntimeException("unsupported method!")
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
      |- Use the configuration account method
      |run command as SendMessage.``
      |where method="mail"
      |and from = "do_not_reply@gmail.com"
      |and content = "${EMAIL_BODY}"
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
      |and content = "${EMAIL_BODY}"
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
      | 
      |- Below we introduce how to use email to send HTML formatted text and carry attachments.
      |
      |1) First, upload two CSV files `employee.csv` and `company.csv` through MLSQL Api Console as attachment content.
      |
      |2) Then, send the email through the following SQL, an example is shown below:
      |
      |```sql
      |set EMAIL_TITLE = "This is the title of the email";
      |set EMAIL_BODY ='''<div>This is the first line</div><br/><hr/><div>This is the second line</div>''';
      |set EMAIL_TO = "yourMailAddress@qq.com";
      |
      |run command as SendMessage.``
      |where method="mail"
      |and content="${EMAIL_BODY}"
      |and from = "yourMailAddress@qq.com"
      |and to = "${EMAIL_TO}"
      |and subject = "${EMAIL_TITLE}"
      |and contentType="text/html"
      |and attachmentContentType="text/csv"
      |and attachmentPaths="/tmp/employee.csv,/tmp/employee.csv"
      |and smtpHost = "smtp.qq.com"
      |and smtpPort="587"
      |and `properties.mail.smtp.starttls.enable`= "true"
      |and `userName`="yourMailAddress@qq.com"
      |and password="---"
      |;
      |```
      |""".stripMargin)
}

object SQLSendMessage {
  private[algs] object MailTypeEnum extends Enumeration {
    type MailTypeEnum = Value
    val CONFIG: Value = Value("config")
    val LOCAL: Value = Value("local")

    def checkExists(optionValues: String): Boolean = this.values.exists(_.toString == optionValues)
  }

  private[algs] object MailContentTypeEnum extends Enumeration {
    type MailContentTypeEnum = Value
    val MIXED: Value = Value("multipart/mixed")
    val TEXT: Value = Value("text/plain")
    val HTML: Value = Value("text/html")
    val CSV: Value = Value("text/csv")
    val JPEG: Value = Value("image/jpeg")
    val DEFAULT_ATTACHMENT: Value = Value("application/octet-stream")
    // Does not support the use of cascading relationships in the email content, including types: RELATED, ALTERNATIVE
    val RELATED: Value = Value("multipart/related")
    val ALTERNATIVE: Value = Value("multipart/alternative")

    def checkExists(optionValues: String): Boolean = this.values.exists(_.toString == optionValues)
  }
}

class MailAgent() extends Logging with WowLog with DslTool {

  // If the value is set, the parameter description can be changed to: required" -> "false"
  final val GLOBAL_USER_NAME: String = null
  final val GLOBAL_IDENTIFY_CODE: String = null

  def sendMessage(to: String,
                  cc: String,
                  bcc: String,
                  from: String,
                  subject: String,
                  content: String,
                  params: Map[String, String],
                  mailContentType: String,
                  attachmentContentType: String,
                  attachmentMap: Map[String, (String, String)],
                  charset: String): Unit = {
    //Build the header
    val message: Message = buildMessageHead(params, from, to, cc, bcc)

    //Build the body
    val body: Multipart = new MimeMultipart

    // Set Content-Type
    val bodyCt = buildContentType(mailContentType, charset)

    //Build these contents
    val contentPart = new MimeBodyPart
    contentPart.setContent(content, bodyCt.toString)
    body.addBodyPart(contentPart)

    //Set some attachment files to Message including the Content-Type
    if (attachmentMap != null && attachmentMap.nonEmpty) {
      import scala.collection.JavaConversions._
      for (entry <- attachmentMap.entrySet) {
        val context = ScriptSQLExec.contextGetOrForTest()
        val allPath = resourceRealPath(context.execListener, Option(context.owner), entry.getKey)
        val (filePath, fileName) = entry.getValue

        val baseDir = resourceRealPath(context.execListener, Option(context.owner), filePath)
        // If `allPath` is a dir, we need to package and upload the file.
        if (HDFSOperatorV2.isDir(allPath)) {
          val output: OutputStream = new ByteArrayOutputStream
          if (DownloadRunner.createTarFileStream(output, allPath) == 200) {
            body.addBodyPart(buildAttachmentBody(parseOutputToInput(output), s"$fileName.tar", attachmentContentType))
          }
        } else if (HDFSOperatorV2.isDir(baseDir) && HDFSOperatorV2.fileExists(Paths.get(baseDir, fileName).toString)) {
          body.addBodyPart(buildAttachmentBody(baseDir, fileName, attachmentContentType))
        }
      }
    }

    // Set Multipart-Content to Message including the Content-Type
    message.setContent(body, buildContentType(MailContentTypeEnum.MIXED.toString, charset).toString)
    message.setSubject(subject)
    Transport.send(message)
  }

  def buildContentType(mailContentType: String, charset: String): ContentType = {
    val ct = new ContentType(mailContentType)
    ct.setParameter("charset", charset)
    ct
  }

  def parseOutputToInput(out: OutputStream): InputStream = {
    val bytes = out.asInstanceOf[ByteArrayOutputStream]
    new ByteArrayInputStream(bytes.toByteArray)
  }

  /**
   * Build the attachment body of the email
   *
   * @param filePath              the path of attachment file
   * @param fileName              the name of attachment file
   * @param attachmentContentType the Content-Type of the attachment file
   * @return
   * @throws Exception
   */
  @throws[Exception]
  private def buildAttachmentBody(filePath: String, fileName: String, attachmentContentType: String): BodyPart = {
    val body: BodyPart = new MimeBodyPart
    body.setDataHandler(new DataHandler(new ByteArrayDataSource(
      HDFSOperatorV2.readAsInputStream(Paths.get(filePath, fileName).toString), attachmentContentType)))
    body.setFileName(MimeUtility.encodeWord(fileName))
    body
  }

  @throws[Exception]
  private def buildAttachmentBody(attachmentStream: InputStream, fileName: String, attachmentContentType: String): BodyPart = {
    val body: BodyPart = new MimeBodyPart
    body.setDataHandler(new DataHandler(new ByteArrayDataSource(
      attachmentStream, attachmentContentType)))
    body.setFileName(MimeUtility.encodeWord(fileName))
    body
  }

  @throws[Exception]
  def buildMessageHead(params: Map[String, String], from: String, to: String, cc: String, bcc: String): MimeMessage = {
    val mailType = params.getOrElse("mailType", MailTypeEnum.CONFIG.toString)
    assert(SQLSendMessage.MailTypeEnum.checkExists(mailType), s"Unsupported mailType: $mailType!")
    val session = createSession(SQLSendMessage.MailTypeEnum.withName(mailType), params)
    val message = new MimeMessage(session)
    if (from != null) {
      message.setFrom(new InternetAddress(from))
    }
    setToCcBccRecipients(message, to, cc, bcc)
    message.setSentDate(new Date())
    message
  }

  def createSession(mailType: MailTypeEnum, params: Map[String, String]): Session = {
    val properties = new Properties
    if (mailType == MailTypeEnum.CONFIG) {
      val smtpHost = params("smtpHost")
      val smtpPort = params.getOrElse("smtpPort", "465")
      require(smtpHost != null)
      properties.setProperty("mail.host", smtpHost)
      properties.setProperty("mail.localhost", smtpHost)
      properties.setProperty("mail.smtp.host", smtpHost)
      properties.setProperty("mail.smtp.localhost", smtpHost)
      properties.setProperty("mail.smtp.auth", "true")
      properties.setProperty("mail.smtp.socketFactory.fallback", "false")
      properties.setProperty("mail.smtp.port", smtpPort)
      properties.setProperty("mail.smtp.socketFactory.port", smtpPort)

      params.filter(_._1.startsWith("properties.")).foreach { case (k, v) =>
        properties.setProperty(k.stripPrefix("properties."), v)
      }

      /**
       * In order to ensure account security and simple configuration, the default secure communication
       * framework standard protocol is SSL. If your mail service provider requires a non-SSL method,
       * please set `properties.mail.smtp.ssl.enable= "false"`, or `properties.mail.smtp.starttls.enable= "false"`
       */
      if (!smtpPort.equals("25") && !properties.containsKey("properties.mail.smtp.starttls.enable")
        && !properties.containsKey("properties.mail.smtp.ssl.enable")) {
        properties.setProperty("properties.mail.smtp.ssl.enable", smtpPort)
        if (!properties.containsKey("mail.smtp.socketFactory.class")) {
          properties.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
        }
      }

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
