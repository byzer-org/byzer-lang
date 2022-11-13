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
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.{AutoSuggestContext, SpecialTableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
 * 15/10/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class SetSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister with StatementUtils {
  private val subSuggesters = mutable.HashMap[String, StatementSuggester]()

  override def tokens: List[Token] = _tokens

  override def tokenPos: TokenPos = _tokenPos

  override def register(clazz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clazz.getConstructor(classOf[SetSuggester]).newInstance(this)
    subSuggesters += instance.name -> instance
    this
  }

  register(classOf[SetOptionsSuggester])
  register(classOf[SetOptionValuesSuggester])

  override def name: String = "set"

  override def suggest(): List[SuggestItem] = {
    keywordSuggest ++ defaultSuggest(subSuggesters.toMap)
  }

  private def keywordSuggest: List[SuggestItem] = {
    var items = List[SuggestItem]()
    // If the where keyword already exists, it will not prompt
    if (backAndFirstIs(DSLSQLLexer.WHERE) || backAndFirstIs(DSLSQLLexer.OPTIONS)) {
      return items
    }

    _tokenPos match {
      case TokenPos(pos, TokenPosType.CURRENT, offsetInToken) if pos > 0 =>
        // The current pos is IDENTIFIER, and is the where prefix
        if ("where".startsWith(_tokens(pos).getText)) {
          items = List(SuggestItem("where ", SpecialTableConst.KEY_WORD_TABLE, Map()))
        } else if ("options".startsWith(_tokens(pos).getText)) {
          items = List(SuggestItem("options ", SpecialTableConst.KEY_WORD_TABLE, Map()))
        }
        items

      case TokenPos(pos, TokenPosType.NEXT, _) if pos > 0 =>

        // The string of set includes a variety of formats, including: STRING, BLOCK_STRING, BACKQUOTED_IDENTIFIER
        val firstIdentifier = if (TokenMatcher.SqlStringIdentsEnum.checkExists(_tokens(pos).getType)) {
          Food(None, _tokens(pos).getType)
        } else if (_tokens(pos).getType == DSLSQLLexer.BACKQUOTED_IDENTIFIER) {
          Food(None, DSLSQLLexer.BACKQUOTED_IDENTIFIER)
        } else {
          Food(None, DSLSQLLexer.STRING)
        }

        val temp = TokenMatcher(_tokens, pos).back
          .eat(firstIdentifier)
          .eat(Food(None, DSLSQLLexer.T__2))
          .eat(Food(None, DSLSQLLexer.IDENTIFIER))
          .build

        if (temp.isSuccess) {
          items = List(
            SuggestItem("where ", SpecialTableConst.KEY_WORD_TABLE, Map()),
            SuggestItem("options ", SpecialTableConst.KEY_WORD_TABLE, Map())
          )
        }
        items

      case _ => List()
    }

  }

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SET) => true
      case _ => false
    }
  }
}

/**
 * Suggest to set the where statement, i.e. where statement contents.
 */
private class SetOptionsSuggester(setSuggester: SetSuggester) extends SetSuggesterBase(setSuggester) {
  val SET_OPTION_SUGGESTIONS = List(
    SuggestItem(SetOptionEnum.TYPE.toString, SpecialTableConst.OPTION_TABLE, Map.empty),
    SuggestItem(SetOptionEnum.MODE.toString, SpecialTableConst.OPTION_TABLE, Map.empty),
    SuggestItem(SetOptionEnum.SCOPE.toString, SpecialTableConst.OPTION_TABLE, Map.empty),
    SuggestItem("and ", SpecialTableConst.KEY_WORD_TABLE, Map.empty)
  )

  override def name: String = "where"

  override def isMatch(): Boolean = {
    if (tokenPos.currentOrNext == TokenPosType.CURRENT && !SET_OPTION_SUGGESTIONS.exists(
      _.name.toUpperCase.startsWith(tokens(tokenPos.pos).getText.toUpperCase))) {
      return false
    }

    val temp = getOptionsIndex
    if (temp == -1) return false
    if (tokens(temp).getType == DSLSQLLexer.WHERE || tokens(temp).getType == DSLSQLLexer.OPTIONS) return true
    false
  }

  override def suggest(): List[SuggestItem] = {
    val temp = getOptionsIndex
    if (temp < tokens.length - 1) {
      val curOptionValue = tokens.slice(temp + 1, tokens.length).filter(item => item.getType == DSLSQLLexer.IDENTIFIER).map(_.getText)
      return SET_OPTION_SUGGESTIONS.filter(item => !curOptionValue.contains(item.name))
    }
    SET_OPTION_SUGGESTIONS
  }
}

private object SetOptionEnum extends Enumeration {
  type SetOptionEnum = Value
  val TYPE: Value = Value("type")
  val MODE: Value = Value("mode")
  val SCOPE: Value = Value("scope")

  def checkExists(optionValues: String): Boolean = this.values.exists(_.toString == optionValues)
}

/**
 * Suggests option values, type includes "text" , "conf", "shell", "sql"
 * , and "defaultParam". mode incoudes "runtime" and "compile".
 */
private class SetOptionValuesSuggester(setSuggester: SetSuggester) extends SetSuggesterBase(setSuggester) {
  override def name: String = "optionValues"

  override def isMatch(): Boolean = {
    if (tokenPos.pos <= 0) {
      return false
    }
    // If string has been set, it will not match
    if (tokenPos.pos + 1 < tokens.length && TokenMatcher.SqlStringIdentsEnum.checkExists(tokens(tokenPos.pos + 1).getType)) {
      return false
    }

    return tokenPos match {
      case TokenPos(_, _, offsetInToken) if tokenPos.pos > 0 =>
        var isStringIdents = TokenMatcher.SqlStringIdentsEnum.checkExists(tokens(tokenPos.pos).getType)

        // Handling UNRECOGNIZED types separately. e.g. set  x=" " where mode="[cursor];
        if (tokens(tokenPos.pos).getType.equals(DSLSQLLexer.UNRECOGNIZED) && TokenMatcher.SqlStringIdentsEnum.checkExistsValue(tokens(tokenPos.pos).getText)) {
          isStringIdents = true
        } else if (isStringIdents && (0 >= offsetInToken || offsetInToken >= tokens(tokenPos.pos).getText.length)) {
          // Outside the token of string, no prompt
          return false
        }

        val skipSize = if (isStringIdents) 1 else 0
        val temp = TokenMatcher(tokens, tokenPos.pos - skipSize).back
          .eat(Food(None, DSLSQLLexer.T__2))
          .eat(Food(None, DSLSQLLexer.IDENTIFIER))
          .build
        temp.isSuccess && SetOptionEnum.checkExists(tokens(tokenPos.pos - skipSize - 1).getText)

      case _ => false
    }
    false
  }

  override def suggest(): List[SuggestItem] = {
    val startIndex = tokenPos.pos
    var isStringIdents = TokenMatcher.SqlStringIdentsEnum.checkExists(tokens(startIndex).getType)
    val isUnRecognized = tokens(tokenPos.pos).getType.equals(DSLSQLLexer.UNRECOGNIZED) && TokenMatcher.
      SqlStringIdentsEnum.checkExistsValue(tokens(tokenPos.pos).getText)
    isStringIdents = isStringIdents || isUnRecognized
    // skipSize means skip forward equal tokens
    val skipSize = if (isStringIdents) 2 else 1
    var suggestItems: List[SuggestItem] = List()
    if (tokens(startIndex - skipSize).getText == SetOptionEnum.TYPE.toString) {
      if (firstAhead(DSLSQLLexer.BACKQUOTED_IDENTIFIER).isDefined) {
        suggestItems = List(
          SuggestItem("\"sql\"", SpecialTableConst.OPTION_TABLE, Map()),
          SuggestItem("\"shell\"", SpecialTableConst.OPTION_TABLE, Map())
        )
      } else {
        suggestItems = List(
          SuggestItem("\"text\"", SpecialTableConst.OPTION_TABLE, Map()),
          SuggestItem("\"conf\"", SpecialTableConst.OPTION_TABLE, Map()),
          SuggestItem("\"shell\"", SpecialTableConst.OPTION_TABLE, Map()),
          SuggestItem("\"sql\"", SpecialTableConst.OPTION_TABLE, Map()),
          SuggestItem("\"defaultParam\"", SpecialTableConst.OPTION_TABLE, Map())
        )
      }
    } else if (tokens(startIndex - skipSize).getText == SetOptionEnum.MODE.toString) {
      suggestItems = List(
        SuggestItem("\"runtime\"", SpecialTableConst.OPTION_TABLE, Map()),
        SuggestItem("\"compile\"", SpecialTableConst.OPTION_TABLE, Map())
      )
    } else if (tokens(startIndex - skipSize).getText == SetOptionEnum.SCOPE.toString) {
      suggestItems = List(
        SuggestItem("\"request\"", SpecialTableConst.OPTION_TABLE, Map()),
        SuggestItem("\"session\"", SpecialTableConst.OPTION_TABLE, Map())
      )
    }
    else {
      //  corner case, shouldn't happen
      return suggestItems
    }
    if (isUnRecognized) {
      return suggestItems.map { item =>
        val startToken = tokens(startIndex)
        val itemName = item.name.substring(1, item.name.length - 1)
        SuggestItem(itemName + startToken.getText, SpecialTableConst.OPTION_TABLE, Map())
      }
    }
    if (isStringIdents) {
      suggestItems = LexerUtils.cleanTokenPrefix(tokens(startIndex).getText, suggestItems)
    }

    suggestItems
  }
}

private abstract class SetSuggesterBase(setSuggester: SetSuggester) extends StatementSuggester with StatementUtils {
  override def tokens: List[Token] = setSuggester._tokens

  override def tokenPos: TokenPos = setSuggester._tokenPos
}
