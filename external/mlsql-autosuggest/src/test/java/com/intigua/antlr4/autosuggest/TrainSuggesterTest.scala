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
import streaming.dsl.mmlib.algs.SQLCacheExt
import tech.mlsql.autosuggest.statement.TrainSuggester
import tech.mlsql.autosuggest.{AutoSuggestContext, TokenPos, TokenPosType}

import java.util.UUID
import scala.collection.JavaConverters._

/**
 * 25/10/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class MockTrainSuggester(val _context: AutoSuggestContext, val _mock_tokens: List[Token], val _mock_token_pos: TokenPos) extends TrainSuggester(_context, _mock_tokens, _mock_token_pos) {
  override def getAllETNames: Set[String] = {
    Set("CacheExt")
  }

  override def getETInstanceMapping: Map[String, SQLAlg] = {
    Map("CacheExt" -> new SQLCacheExt(UUID.randomUUID().toString))
  }
}

class TrainSuggesterTest extends BaseTest {
  test("run c[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | run c
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockTrainSuggester(context, statement, TokenPos(1, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("command as "))
  }

  test("run command as Cache[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | run command as Cache
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockTrainSuggester(context, statement, TokenPos(3, TokenPosType.CURRENT, 5)).suggest()
    assert(suggestions.map(_.name).toSet == Set("CacheExt.`` "))
  }

  test("run command as CacheExt.`` w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | run command as CacheExt.`` w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new MockTrainSuggester(context, statement, TokenPos(6, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }
}
