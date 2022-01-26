package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.{Code, Doc, HtmlDoc, SQLAlg, SQLCode}
import streaming.dsl.mmlib.algs.SQLSendMessage.MailContentTypeEnum
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Dynamic, Extra, FormParams, KV, Select, Text}
import tech.mlsql.dsl.adaptor.DslTool


class SQLSendMultiMails(override val uid: String) extends SQLAlg with Functions with BaseParams with DslTool {

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
            | Can be only used after `where` statement
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

  override def doc: Doc = Doc(HtmlDoc,
    """
      | SendMultiMails provides the ability to send multiple mails externally. The mail service provides two ways, one is to configure the email account of the mail sender through MLSQL and directly connect to the SMTP service to send mail, and the other is to connect to the local sendmail service to send mail.
      |
      |scenes to be used:
      |1. After the data is calculated and processed, send the result to multiple users
      |2. When the amount of data is small, the data processing result can be sent directly
      |
      |configuration:
      |1. Set parameters into a table and specify it through the 'paramTab' parameter. Make Sure column names equals to parameter names.
      |2. Set parameters after `where` statement as global config. And parameter with the same name will be overwritten.
      |
      | Use "load modelParams.`SendMultiMails` as output;"
      | to check the available parameters;
      |
      | Use "load modelExample.`SendMultiMails` as output;"
      | get example.
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      | Table named 'send_mail_config' like this
      || to              | subject         | title    | attachment   | attachmentType | content |
      || --------------- | --------------- | -------- | ------------ | -------------- | ------- |
      || test1@gmail.com | test_send_email | email_01 | (csv string) | text/csv       | test01  |
      || test2@gmail.com | test_send_email | email_02 | (csv string) | text/csv       | test02  |
      |
      |Provide code for sending multiple mails. As follows:
      |
      | ```sql
      | set EMAIL_FROM = "do_not_reply@gmail.com";
      | set HOST = "smtp.gmail.com";
      | set PORT = "465"
      | set USERNAME = "username";
      | set PWD = "password";
      |
      |run command as SendMultiMails.``
      |where paramTab ="send_mail_config"
      |and from = "${EMAIL_FROM}"
      |and smtpHost = "${HOST}"
      |and `userName`="${USERNAME}"
      |and password="${PASSWORD}"
      |;
      |```
      |""".stripMargin)

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  // table col: to, cc, subject, attachment, attachmentType
  // where params: smtpHost, smtpPort, mailType, userName, password, content, contentType, charset
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    var paramMap = params
    df.collect().foreach { row =>
      def getParamOrElse(key: String, default: String):String = {
        var value: String = null
        try {
          value = params.getOrElse(key, row.getAs[String](key))
        } catch {
          case ex: Exception => {
            logWarning(s"row [${key}] is not exist, use default value [${default}]")
            value = default
          }
        }
        paramMap += (key -> value)
        value
      }

      val to = getParamOrElse("to", null)
      val cc = getParamOrElse("cc", null)
      val subject = getParamOrElse("subject", "flag-platform-email")
      val attachmentType = getParamOrElse("attachmentType", "application/octet-stream")
      val attachment = getParamOrElse("attachment", null)
      // Set default value if param is not set by user
      getParamOrElse("smtpHost", null)
      getParamOrElse("smtpPort", "465")
      getParamOrElse("userName", null)
      getParamOrElse("password", null)
      val content = getParamOrElse("content", null)
      val from = getParamOrElse("from", null)
      val contentType = getParamOrElse("contentType", "text/plain")
      val charset = getParamOrElse("charset", "utf-8")

      require(to != null, "the parameter [to] cannot be empty!")
      logInfo(format(s"send content: $content to $to"))
      MailAgent.sendMessage(to, cc, null, from, subject, content, paramMap,
        contentType, attachmentType, null, attachment, charset)
    }
emptyDataFrame()(df)
  }
}
