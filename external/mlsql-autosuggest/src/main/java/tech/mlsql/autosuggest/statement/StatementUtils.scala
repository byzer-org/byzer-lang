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
package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}

/**
 * 9/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
trait StatementUtils {

  def tokens: List[Token]

  def tokenPos: TokenPos

  def SPLIT_KEY_WORDS = {
    List(DSLSQLLexer.OPTIONS, DSLSQLLexer.WHERE, DSLSQLLexer.AS)
  }

  def backAndFirstIs(t: Int, keywords: List[Int] = SPLIT_KEY_WORDS): Boolean = {
    // 从光标位置去找第一个核心词
    val temp = TokenMatcher(tokens, tokenPos.pos).back.orIndex(keywords.map(Food(None, _)).toArray)
    if (temp == -1) return false
    //第一个核心词必须是指定的词
    if (tokens(temp).getType == t) return true
    return false
  }

  def firstAhead(targetType: Int*): Option[Int] = {
    val targetFoods = targetType.map(Food(None, _)).toArray
    val matchingResult = TokenMatcher(tokens, tokenPos.pos)
      .back
      .orIndex(targetFoods)
    if (matchingResult >= 0) {
      Some(matchingResult)
    } else {
      None
    }
  }

  def getOptionsIndex: Int = {
    TokenMatcher(tokens, tokenPos.pos).back.orIndex(List(Food(None, DSLSQLLexer.WHERE),
      Food(None, DSLSQLLexer.OPTIONS)).toArray)
  }
}

object StatementUtils {
  val SUGGEST_FORMATS = Seq(
    "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
    "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
  )
}
