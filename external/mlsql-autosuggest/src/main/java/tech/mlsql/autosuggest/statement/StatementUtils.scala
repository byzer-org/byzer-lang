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
import tech.mlsql.autosuggest.dsl.{DSLWrapper, Food, TokenMatcher}
import tech.mlsql.autosuggest.{SpecialTableConst, TokenPos, TokenPosType}

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

  def currentTokenIs(t: Int): Boolean = {
    tokens(tokenPos.pos).getType == t
  }

  def outOfToken: Boolean = {
    tokenPos.currentOrNext == TokenPosType.NEXT
  }

  // a [cursor]  then target is a
  // a b[cursor] the target is also is a
  def backOneStepIs(t: Int): Boolean = {
    tokenPos.currentOrNext match {
      case TokenPosType.NEXT =>
        tokens(tokenPos.pos).getType == t
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None, t)).isSuccess
    }
  }

  // a b [cursor]  then target is a
  // a b c[cursor] the target is also is a
  def backTwoStepIs(t: Int): Boolean = {
    tokenPos.currentOrNext match {
      case TokenPosType.NEXT =>
        TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None,t)).isSuccess
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eatOneAny.eat(Food(None, t)).isSuccess
    }
  }

  def aheadOneStepIs(t: Int): Boolean = {
    TokenMatcher(tokens, tokenPos.pos).forward.eat(Food(None, t)).isSuccess
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

  // where key[cursor]
  // where key="" and key[cursor]
  // where `key[cursor]
  def isOptionKey = {
    tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None, DSLWrapper.AND)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None, DSLSQLLexer.WHERE)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None, DSLSQLLexer.OPTIONS)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(Some("`"), DSLSQLLexer.UNRECOGNIZED)).isSuccess
      case TokenPosType.NEXT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLWrapper.AND)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLSQLLexer.WHERE)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLSQLLexer.OPTIONS)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eat(Food(Some("`"), DSLSQLLexer.UNRECOGNIZED)).isSuccess

    }

  }

  def isQuoteShouldPromb: Boolean = {
    isOptionValue && !isInQuote
  }

  def getQuotes = {
    List(
      SuggestItem("\"\"", SpecialTableConst.OPTION_TABLE, Map("desc" -> "")),
      SuggestItem("''''''", SpecialTableConst.OPTION_TABLE, Map("desc" -> ""))
    )
  }

  def getOptionsKeywords = {
    List(
      SuggestItem("where", SpecialTableConst.OPTION_TABLE, Map()),
      SuggestItem("options", SpecialTableConst.OPTION_TABLE, Map())
    )
  }

  def isOptionKeywordShouldPromp(food: Food*): Boolean = {
    if (backAndFirstIs(DSLSQLLexer.OPTIONS) || backAndFirstIs(DSLSQLLexer.WHERE)) {
      return false
    }
    val promp = tokenPos.currentOrNext match {
      // load jdbc.`` w[cursor]
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(food: _*).isSuccess
      // load jdbc.`` [cursor]
      case TokenPosType.NEXT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(food: _*).isSuccess
    }
    promp
  }

  // where key=[cursor]
  // where key="" and key1=[cursor]
  // where `key`=[cursor]
  def isOptionValue = {
    tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLWrapper.EQUAL)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eatOneAny.eat(Food(None, DSLWrapper.EQUAL)).isSuccess
      case TokenPosType.NEXT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLWrapper.EQUAL)).isSuccess
    }
  }

  // where key="" and key1="[cursor]"
  def isOptionKeyEqual(name: String): Boolean = {
    if (isOptionValue) {
      if (isInQuote) {
        return tokens(tokenPos.pos - 2).getText == name
      } else {
        return tokens(tokenPos.pos - 1).getText == name
      }
    }
    return false

  }

  def isInQuote = {
    tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLSQLLexer.STRING)).isSuccess ||
          TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLSQLLexer.BLOCK_STRING)).isSuccess
      case TokenPosType.NEXT =>
        TokenMatcher(tokens, tokenPos.pos).back.eat(Food(None, DSLWrapper.EQUAL)).isSuccess
    }
  }


}

object StatementUtils {
  val SUGGEST_FORMATS = Seq(
    "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
    "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
  )

  val LOAD_STRING_FORMATS = Seq(
    "jsonStr", "csvStr", "script"
  )

  def extractSetStatement(statements: List[List[Token]], format: String): Option[List[List[Token]]] = {
    var currentIndex = -1
    for (i <- statements.indices) {
      if (currentIndex == -1) {
        val tokens = statements(i)
        tokens.headOption.map(_.getText) match {
          case Some("set") =>
          //continue
          case Some(_) =>
            if (tokens.map(_.getText.equals(format)).nonEmpty) {
              currentIndex = i
            }
          case None =>
          //continue
        }
      }
    } //for

    if (currentIndex == -1) {
      return None
    }

    val slice = statements.slice(0, currentIndex).filter(_.headOption.map(_.getText.equals("set")).isDefined)
      .filter(tokens => {
        var res: Boolean = false
        val index = TokenMatcher(tokens, tokens.length - 1).back.orIndex(Array(Food(None, DSLSQLLexer.WHERE)))
        if (index == -1) {
          res = true
        } else {
          res = (
            TokenMatcher(tokens, tokens.length - 1).back
              .eat(Food(None, DSLWrapper.SEMICOLON))
              .eat(Food(None, DSLSQLLexer.BLOCK_STRING), Food(None, DSLWrapper.EQUAL))
              .eat(Food(None, DSLSQLLexer.IDENTIFIER))
              .build.isSuccess ||
              TokenMatcher(tokens, tokens.length - 1).back
                .eat(Food(None, DSLWrapper.SEMICOLON))
                .eat(Food(None, DSLSQLLexer.STRING), Food(None, DSLWrapper.EQUAL))
                .eat(Food(None, DSLSQLLexer.IDENTIFIER))
                .build.isSuccess
            ) &&
            // The `set` syntax has type, and it is not of `text` or `defaultParam` type, so there is no syntax hint.
            (!tokens.exists(_.getText.equals("type")) || tokens.exists(token => LexerUtils.cleanStr(token.getText).equals("text") || token.getText.equals("defaultParam"))) &&
            // The `set` syntax has mode, and it is not of `compile` type, so there is no syntax hint.
            (!tokens.exists(_.getText.equals("mode")) || tokens.exists(token => LexerUtils.cleanStr(token.getText).equals("compile")))
        }
        res
      })
    if (slice.isEmpty) {
      return None
    }

    Option(slice)
  }
}
