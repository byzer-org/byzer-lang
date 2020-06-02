package com.intigua.antlr4.autosuggest

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLParser}
import tech.mlsql.atuosuggest.AutoSuggestContext
import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class BaseTest extends FunSuite with BeforeAndAfterAll {
  var context: AutoSuggestContext = _
  var tokens: List[Token] = _

  override def beforeAll(): Unit = {
    val sparkSession = SparkSession.builder().appName("local").master("local[*]").getOrCreate()


    val lexerAndParserfactory = new ReflectionLexerAndParserFactory(classOf[DSLSQLLexer], classOf[DSLSQLParser]);
    val loadLexer = new LexerWrapper(lexerAndParserfactory)
    context = new AutoSuggestContext(sparkSession, loadLexer)
    var tr = loadLexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        |""".stripMargin)
    tokens = tr.tokens.asScala.toList
  }

  override def afterAll(): Unit = {
    context.session.close()
  }

}
