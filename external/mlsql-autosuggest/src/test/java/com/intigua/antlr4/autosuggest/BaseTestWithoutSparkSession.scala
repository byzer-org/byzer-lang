package com.intigua.antlr4.autosuggest

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import tech.mlsql.autosuggest.app.AutoSuggestController
import tech.mlsql.autosuggest.{AutoSuggestContext, MLSQLSQLFunction}

/**
 * 11/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class BaseTestWithoutSparkSession extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  var context: AutoSuggestContext = _


  override def beforeAll(): Unit = {

  }

  override def afterAll(): Unit = {
    context.session.close()
  }


  override def beforeEach(): Unit = {
    MLSQLSQLFunction.funcMetaProvider.clear
    context = new AutoSuggestContext(null, AutoSuggestController.mlsqlLexer, AutoSuggestController.sqlLexer)
    context.setDebugMode(true)
  }

}
