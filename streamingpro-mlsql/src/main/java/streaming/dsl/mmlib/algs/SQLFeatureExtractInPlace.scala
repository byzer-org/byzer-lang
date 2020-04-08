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

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst.getMetaPath

/**
 * Created by dxy_why on 2018/8/20.
 */
class SQLFeatureExtractInPlace extends SQLAlg with Functions {

  /**
   * 是否含有电话
   *
   * @return
   */
  def phoneExisted = F.udf((doc: String) => {
    SQLFeatureExtractInPlace.EXISTED_PHONE_REGEX.map(regex => {
      val pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
      val matcher = pattern.matcher(doc)
      matcher.find()
    }).contains(true)
  })

  /**
   * 是否含有邮箱地址
   *
   * @return
   */
  def emailExisted = F.udf((doc: String) => {
    SQLFeatureExtractInPlace.EXISTED_EMAIL_REGEX.map(regex => {
      val pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
      val matcher = pattern.matcher(doc)
      matcher.find()
    }).contains(true)
  })

  /**
   * 是否还有qq微信号
   *
   * @return
   */
  def qqwechatExisted = F.udf((doc: String) => {
    SQLFeatureExtractInPlace.EXISTED_QQWECHAT_REGEX.map(regex => {
      val pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
      val matcher = pattern.matcher(doc)
      matcher.find()
    }).contains(true)
  })

  /**
   * url数量
   *
   * @return
   */
  def urlNumber = F.udf((doc: String) => {
    StringUtils.countMatches(doc, "http")
  })

  /**
   * 图片数量
   *
   * @return
   */
  def picNumber = F.udf((doc: String) => {
    SQLFeatureExtractInPlace.PIC_NUMBER_REGEX.map(regex => {
      val pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
      val matcher = pattern.matcher(doc)
      var count = 0
      while (matcher.find()) {
        count += 1
      }
      count
    }).head
  })

  def cleanEmotionAndSpecChar = F.udf((doc: String) => {
    val regEx_emotion = """<img src="http://assets.dxycdn.com(.*?)/>"""
    val p_emotion = Pattern.compile(regEx_emotion, Pattern.CASE_INSENSITIVE)
    val m_emotion = p_emotion.matcher(doc)
    val htmlStr = m_emotion.replaceAll("").replaceAll("&lt;", "<").replaceAll("&gt;", ">")
      .replaceAll("&amp;", "&").replaceAll("&#64;", "@")
    /**
     * 去除新版app内自带的用户标签
     */
    val regEx_user = """<div class="quote"><blockquote>[\s\S]*?</blockquote>"""
    val p_user = Pattern.compile(regEx_user, Pattern.CASE_INSENSITIVE)
    val m_user = p_user.matcher(htmlStr)
    m_user.replaceAll("")

  })

  def cleanDoc = F.udf((doc: String) => {
    /**
     * 去除html标签
     */
    // 定义script的正则表达式
    val regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"
    // 定义style的正则表达式
    val regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"
    // 定义HTML标签的正则表达式
    val regEx_html = "<[^>]+>"
    // 过滤script标签
    val p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE)
    val m_script = p_script.matcher(doc)
    var htmlStr = m_script.replaceAll("")
    // 过滤style标签
    val p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE)
    val m_style = p_style.matcher(htmlStr)
    htmlStr = m_style.replaceAll("")
    // 过滤html标签
    val p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE)
    val m_html = p_html.matcher(htmlStr)
    htmlStr = m_html.replaceAll("")
    var cleanedDoc = htmlStr.trim().replaceAll("\r|\n|\t", "")
      .replaceAll("<[^>]*>", "").replaceAll("&nbsp", "")

    /**
     * 去除url
     */
    val urlPattern = Pattern.compile(SQLFeatureExtractInPlace.URL_NUMBER_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val urlMatcher = urlPattern.matcher(cleanedDoc)
    cleanedDoc = urlMatcher.replaceAll("")

    /**
     * 去除邮箱
     */
    val emailPattern = Pattern.compile(SQLFeatureExtractInPlace.EXISTED_EMAIL_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val emailMatcher = emailPattern.matcher(cleanedDoc)
    cleanedDoc = emailMatcher.replaceAll("")

    /**
     * 去除图片链接
     */
    val picPattern = Pattern.compile(SQLFeatureExtractInPlace.PIC_NUMBER_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val picMatcher = picPattern.matcher(cleanedDoc)
    cleanedDoc = picMatcher.replaceAll("")

    cleanedDoc

  })

  /**
   * 文本长度
   *
   * @return
   */
  def docLength = F.udf((doc: String) => {
    doc.length
  })


  /**
   * 空格百分比
   *
   * @return
   */
  def blankPercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.BLANK_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      count * 100 / doc.length
    } else {
      0
    }
  })

  /**
   * 中文百分比
   *
   * @return
   */
  def chinesePercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.CHINESE_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      count * 100 / doc.length
    } else {
      0
    }
  })

  /**
   * 英文百分比
   *
   * @return
   */
  def englishPercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.ENGLISH_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      count * 100 / doc.length
    } else {
      0
    }
  })

  /**
   * 数字百分比
   *
   * @return
   */
  def numberPercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.NUMBER_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      count * 100 / doc.length
    } else {
      0
    }
  })

  /**
   * 标点百分比
   *
   * @return
   */
  def punctuationPercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.PUNCTUATION_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      count * 100 / doc.length
    } else {
      0
    }
  })


  /**
   * 不可见字符百分比
   * @return
   */
  def uninvislblePercent = F.udf((doc: String) => {
    val pattern = Pattern.compile(SQLFeatureExtractInPlace.UNINVISIBLE_PERCENT_REGEX(0),
      Pattern.CASE_INSENSITIVE & Pattern.DOTALL)
    val matcher = pattern.matcher(doc)
    var count = 0
    while (matcher.find()) {
      count += 1
    }
    if (doc.length != 0) {
      // 特殊字符长度为2
      count * 100 * 2 / doc.length
    } else {
      0
    }
  })

  /**
   * 出现最多字符百分比
   *
   * @return
   */
  def mostcharPercent = F.udf((doc: String) => {
    doc.split("").map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).maxBy(_._2)._2
  })

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val path = params("path")
    val metaPath = getMetaPath(path)
    saveTraningParams(df.sparkSession, params, metaPath)
    val featureCol = params.getOrElse(SQLFeatureExtractInPlace.INPUTCOL, SQLFeatureExtractInPlace.FEATUREC_COL)
    val featureExtractResult = df.withColumn("phone", phoneExisted(F.col(featureCol)))
      .withColumn("noEmoAndSpec", cleanEmotionAndSpecChar(F.col(featureCol)))
      .withColumn("email", emailExisted(F.col("noEmoAndSpec")))
      .withColumn("qqwechat", qqwechatExisted(F.col("noEmoAndSpec")))
      .withColumn("url", urlNumber(F.col("noEmoAndSpec")))
      .withColumn("pic", picNumber(F.col("noEmoAndSpec")))
      .withColumn("cleanedDoc", cleanDoc(F.col("noEmoAndSpec")))
      .withColumn("blank", blankPercent(F.col("cleanedDoc")))
      .withColumn("chinese", chinesePercent(F.col("cleanedDoc")))
      .withColumn("english", englishPercent(F.col("cleanedDoc")))
      .withColumn("number", numberPercent(F.col("cleanedDoc")))
      .withColumn("punctuation", punctuationPercent(F.col("cleanedDoc")))
      .withColumn("uninvisible", uninvislblePercent(F.col("cleanedDoc")))
      .withColumn("mostchar", mostcharPercent(F.col("cleanedDoc")))
      .withColumn("length", docLength(F.col("cleanedDoc")))
    featureExtractResult.write.mode(SaveMode.Overwrite).parquet(MetaConst.getDataPath(path))
  }


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    internal_train(df, params + ("path" -> path))
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    AnyRef
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    sparkSession.udf.register(name + "_phone", phoneExisted)
    sparkSession.udf.register(name + "_email", emailExisted)
    sparkSession.udf.register(name + "_qqwechat", qqwechatExisted)
    sparkSession.udf.register(name + "_url", urlNumber)
    sparkSession.udf.register(name + "_cleanEmotionAndSpec", cleanEmotionAndSpecChar)
    sparkSession.udf.register(name + "_cleanHtml", cleanDoc)
    sparkSession.udf.register(name + "_pic", picNumber)
    sparkSession.udf.register(name + "_blank", blankPercent)
    sparkSession.udf.register(name + "_chinese", chinesePercent)
    sparkSession.udf.register(name + "_english", englishPercent)
    sparkSession.udf.register(name + "_number", numberPercent)
    sparkSession.udf.register(name + "_punctuation", punctuationPercent)
    sparkSession.udf.register(name + "_mostchar", mostcharPercent)
    sparkSession.udf.register(name + "_length", docLength)
    phoneExisted
  }
}

object SQLFeatureExtractInPlace {
  /**
   * 输入列名
   */
  val INPUTCOL = "inputCol"
  /**
   * 待提取字段的文本字段名称
   */
  val FEATUREC_COL = "doc"
  /**
   * 参数数组前缀
   */
  val PARAMS_PREFIX = "fitParam"
  /**
   * 是否含有电话的正则表达式规则
   */
  val EXISTED_PHONE_REGEX = Seq(
    """\D[01]{1}\d{2,3}-?\d{8}\D""",
    """(热线|电话|TEL|来电|短信|手机).{0,3}\d{3,}""",
    """400-?\d{3}-?\d{3}""",
    """(热线|电话|TEL|来电|手机).{0,3}\d{3,}[- ]?\d{3,}[- ]?\d{3,}""",
    """\D1\d{2}\s*\D?\s*\d{4}\s*\D?\s*\d{4}""",
    """\D1\d{3}\s*\D?\s*\d{4}\s*\D?\s*\d{3}""")
  /**
   * 是否含有邮箱地址的正则表达式规则
   */
  val EXISTED_EMAIL_REGEX =
    Seq("""[\*a-zA-Z0-9-_]+@[a-zA-Z0-9]+\.[a-zA-Z]+""")
  /**
   * 是否还有qq微信号的正则表达式规则
   */
  val EXISTED_QQWECHAT_REGEX = Seq(
    """(加|Q|群|加我|扣扣)[^\w]{0,10}\d{5,}""",
    """(Q|群|加我|扣扣)[^\w]{0,10}(\d[\- ]?){5,}""",
    """(微信|v信|V)[^\w]{0,8}[a-zA-Z0-9]{5,}""")
  /**
   * url数量的正则表达式规则
   */
  val URL_NUMBER_REGEX = Seq("""(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]""")
  /**
   * 图片数量的正则表达式规则
   */
  val PIC_NUMBER_REGEX = Seq("""img.dxycdn.com""")

  /**
   * 空格百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val BLANK_PERCENT_REGEX = Seq("""[\\s|\r|\n|\t| ]""")

  /**
   * 中文百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val CHINESE_PERCENT_REGEX = Seq("""[\u4e00-\u9fa5]""")

  /**
   * 英文百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val ENGLISH_PERCENT_REGEX = Seq("""[a-zA-Z]""")

  /**
   * 数字百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val NUMBER_PERCENT_REGEX = Seq("""[0-9]""")

  /**
   * 标点百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val PUNCTUATION_PERCENT_REGEX = Seq("""\p{P}""")

  /**
   * 出现最多字符百分比的正则表达式规则（去除html标签, 邮箱, url, 图片链接后）
   */
  val MOSTCHAR_PERCENT_REGEX = Seq("""\w+""")

  /**
   * 不可见字符百分比的正则表达式（去除html标签, 邮箱, url, 图片链接后）
   */
  val UNINVISIBLE_PERCENT_REGEX = Seq("""\p{C}""")
}