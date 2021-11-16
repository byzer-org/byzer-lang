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
package com.intigua.antlr4.autosuggest

import org.antlr.v4.runtime.Token
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLRandomForest
import tech.mlsql.autosuggest.SpecialTableConst.TEMP_TABLE_DB_KEY
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableKey}
import tech.mlsql.autosuggest.statement.RegisterSuggester
import tech.mlsql.autosuggest.{AutoSuggestContext, TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 16/11/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class MockRegisterSuggester(val _context: AutoSuggestContext, val _mock_tokens: List[Token], val _mock_token_pos: TokenPos) extends RegisterSuggester(_context, _mock_tokens, _mock_token_pos) {
  override def getAllETNames: Set[String] = {
    Set("RandomForest")
  }

  override def getETInstanceMapping: Map[String, SQLAlg] = {
    Map("RandomForest" -> new SQLRandomForest)
  }
}

class RegisterSuggesterTest extends BaseTest {
  // register RandomForest.`/tmp/model` as rf_predict where
  // algIndex="0";
  test("register RandomFores[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | register RandomFores
        |""".stripMargin).tokens.asScala.toList
    val suggester = new MockRegisterSuggester(context, statement, TokenPos(1, TokenPosType.CURRENT, 11))
    val suggestions = suggester.suggest()
    assert(suggestions.map(_.name).toSet == Set("RandomForest."))
  }

  test("register RandomForest.[cursor]") {
    val tableName = "data"
    val pathQuote = "/tmp/model"
    val metaTable = MetaTable(MetaTableKey(None, Option(TEMP_TABLE_DB_KEY), tableName, pathQuote), List())
    context.modelProvider.register(tableName, metaTable)
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | register RandomForest.
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockRegisterSuggester(context, statement, TokenPos(2, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("`/tmp/model` as "))
  }

  test("register RandomForest.`/tmp/model` as rr o[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | register RandomForest.`/tmp/model` as rr o
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockRegisterSuggester(context, statement, TokenPos(6, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("options "))
  }

  test("register RandomForest.`/tmp/model` as rr w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | register RandomForest.`/tmp/model` as rr w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockRegisterSuggester(context, statement, TokenPos(6, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }

  test("register RandomForest.`/tmp/model` as rr options a[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | register RandomForest.`/tmp/model` as rr options a
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockRegisterSuggester(context, statement, TokenPos(7, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("and ", "`algIndex`=", "`autoSelectByMetric`="))
  }
}
