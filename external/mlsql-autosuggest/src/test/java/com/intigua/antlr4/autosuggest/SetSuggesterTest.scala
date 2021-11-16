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

import tech.mlsql.autosuggest.statement.SetSuggester
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}

import scala.collection.JavaConverters._

/**
 * 15/10/2021 hellozepp(lisheng.zhanglin@163.com)
 */
class SetSuggesterTest extends BaseTest {
  test("set hello='world' w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='world' w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }

  test("set hello=\"world\" w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello="world" w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }

  test("set hello='''world''' w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='''world''' w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }

  test("set hello='''world''' o[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='''world''' o
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("options "))
  }

  test("set date=`date` w[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set date=`date` w
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(4, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("where "))
  }

  test("set date=`date` where m[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set date=`date` where m
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(5, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("mode", "type", "and "))
  }

  test("set date=`date` where type=[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set date=`date` where type=
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(6, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("\"shell\""))
  }

  test("set hello='world' where type=[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='world' where type=
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(6, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("\"text\"", "\"conf\"", "\"shell\"", "\"sql\"", "\"defaultParam\""))
  }


  test("set hello='world' where type=\"[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='world' where type="
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(7, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("text\"", "conf\"", "shell\"", "sql\"", "defaultParam\""))
  }

  test("set hello='world' where type=\"[cursor]\"") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='world' where type=""
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(7, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("text", "conf", "shell", "sql", "defaultParam"))
  }

  test("set hello='world' where type=\"text\" a[cursor]") {
    val statement = context.lexer.tokenizeNonDefaultChannel(
      """
        | -- yes
        | set hello='world' where type="text" a
        |""".stripMargin).tokens.asScala.toList

    val suggestions = new SetSuggester(context, statement, TokenPos(8, TokenPosType.CURRENT, 1)).suggest()
    assert(suggestions.map(_.name).toSet == Set("and ", "mode"))
  }
}
