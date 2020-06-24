package com.intigua.antlr4.autosuggest

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.{SqlBaseLexer, SqlBaseParser}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLParser}
import tech.mlsql.autosuggest.{AutoSuggestContext, MLSQLSQLFunction}

import scala.collection.JavaConverters._

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class BaseTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  val lexerAndParserfactory = new ReflectionLexerAndParserFactory(classOf[DSLSQLLexer], classOf[DSLSQLParser]);
  val loadLexer = new LexerWrapper(lexerAndParserfactory, new DefaultToCharStream)

  val lexerAndParserfactory2 = new ReflectionLexerAndParserFactory(classOf[SqlBaseLexer], classOf[SqlBaseParser]);
  val rawSQLloadLexer = new LexerWrapper(lexerAndParserfactory2, new RawSQLToCharStream)

  var context: AutoSuggestContext = _
  var tokens: List[Token] = _
  var sparkSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkSession = SparkSession.builder().appName("local").master("local[*]").getOrCreate()

  }

  override def afterAll(): Unit = {
    context.session.close()
  }


  override def beforeEach(): Unit = {
    MLSQLSQLFunction.funcMetaProvider.clear
    context = new AutoSuggestContext(sparkSession, loadLexer, rawSQLloadLexer)
    context.setDebugMode(true)
    var tr = loadLexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | load hive.`` as -- jack
        | table1;
        |""".stripMargin)
    tokens = tr.tokens.asScala.toList
  }

  def getMLSQLTokens(sql: String) = {
    context.lexer.tokenizeNonDefaultChannel(sql).tokens.asScala.toList
  }

}
