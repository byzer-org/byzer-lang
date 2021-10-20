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
import javax.mail.{Address, Message, Session, Transport}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLSendMessage._
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.common.form.{Extra, FormParams, KV, Select, Text}

/**
  * Created by fchen on 2018/8/22.
  */
class SQLSendMessage(override val uid: String) extends SQLAlg with Functions with BaseParams {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val messageCount = df.count()
    require(messageCount <= MAX_MESSAGE_THRESHOLD, s"message count should <= ${MAX_MESSAGE_THRESHOLD}!")

    val to = params("to")
    val from = params.getOrElse("from", null)
    val subject = params.getOrElse("subject", "no subject!")
    val smtpHost = params("smtpHost")
    val smtpPort = params.getOrElse("smtpPort", "-1")
    val method = params("method")

    require(to != null, "to is null!")
    require(smtpHost != null)
    val agent = new MailAgent(smtpHost, smtpPort)
    if (df.count() == 0) {
      method.toUpperCase() match {
        case "MAIL" =>
          logInfo(s"send content: ${$(content)} to ${to}")
          agent.sendMessage(to, null, null, from, subject, $(content))
        case _ =>
          throw new RuntimeException("unspport method!")
      }
    } else {
      df.toJSON.collect().foreach(contentJson => {
        method.toUpperCase() match {
          case "MAIL" =>
            logInfo(s"send content: ${contentJson} to ${to}")
            agent.sendMessage(to, null, null, from, subject, contentJson)
          case _ =>
            throw new RuntimeException("unspport method!")
        }
      })
    }

    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support predict function.")
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  final val from: Param[String]  = new Param[String] (this, "from",
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

  final val to: Param[String]  = new Param[String] (this, "to",
    FormParams.toJson(Text(
      name = "to",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. The target email
          """,
        label = "The target email",
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

  final val subject: Param[String]  = new Param[String] (this, "subject",
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

  final val smtpHost: Param[String]  = new Param[String] (this, "smtpHost",
    FormParams.toJson(Text(
      name = "smtpHost",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. Server of email
          """,
        label = "Email Server",
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

  final val smtpPort: Param[String] = new Param[String](this, "smtpPort",
    FormParams.toJson(Text(
      name = "smtpPort",
      value = "",
      extra = Extra(
        doc =
          """
            | Required. Server port of email
          """,
        label = "Email Server Port",
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

  final val method: Param[String]  = new Param[String] (this, "method",
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

  final val content: Param[String]  = new Param[String] (this, "content",
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

  override def explainParams(sparkSession: SparkSession): DataFrame = _explainParams(sparkSession)
}

object SQLSendMessage {
  val MAX_MESSAGE_THRESHOLD = 10

}

class MailAgent(smtpHost: String, smtpPort : String) {

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
    properties.put("mail.host", smtpHost)
    properties.put("mail.localhost", smtpHost)
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.localhost", smtpHost)
    properties.put("mail.smtp.auth", "false")
    properties.setProperty("mail.smtp.socketFactory.fallback", "false")
    properties.setProperty("mail.smtp.port", smtpPort)
    properties.setProperty("mail.smtp.socketFactory.port", smtpPort)
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
