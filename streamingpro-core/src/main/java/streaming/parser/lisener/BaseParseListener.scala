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

package streaming.parser.lisener

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.{ErrorNode, TerminalNode}
import streaming.dsl.parser.DSLSQLListener
import streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 11/9/2018.
  */
abstract class BaseParseListener extends DSLSQLListener {

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#rawCommandValue}.
    *
    * @param ctx the parse tree
    */
  override def enterRawCommandValue(ctx: RawCommandValueContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#rawCommandValue}.
    *
    * @param ctx the parse tree
    */
  override def exitRawCommandValue(ctx: RawCommandValueContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#as}.
    *
    * @param ctx the parse tree
    */
  override def enterAs(ctx: AsContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#as}.
    *
    * @param ctx the parse tree
    */
  override def exitAs(ctx: AsContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#into}.
    *
    * @param ctx the parse tree
    */
  override def enterInto(ctx: IntoContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#into}.
    *
    * @param ctx the parse tree
    */
  override def exitInto(ctx: IntoContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#saveMode}.
    *
    * @param ctx the parse tree
    */
  override def enterSaveMode(ctx: SaveModeContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#saveMode}.
    *
    * @param ctx the parse tree
    */
  override def exitSaveMode(ctx: SaveModeContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#where}.
    *
    * @param ctx the parse tree
    */
  override def enterWhere(ctx: WhereContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#where}.
    *
    * @param ctx the parse tree
    */
  override def exitWhere(ctx: WhereContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#whereExpressions}.
    *
    * @param ctx the parse tree
    */
  override def enterWhereExpressions(ctx: WhereExpressionsContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#whereExpressions}.
    *
    * @param ctx the parse tree
    */
  override def exitWhereExpressions(ctx: WhereExpressionsContext): Unit = {}

  /**
    * Enter a parse tree produced by {@link DSLSQLParser#commandValue}.
    *
    * @param ctx the parse tree
    */
  override def enterCommandValue(ctx: CommandValueContext): Unit = {}

  /**
    * Exit a parse tree produced by {@link DSLSQLParser#commandValue}.
    *
    * @param ctx the parse tree
    */
  override def exitCommandValue(ctx: CommandValueContext): Unit = {}

  override def enterStatement(ctx: StatementContext): Unit = {}

  override def exitStatement(ctx: StatementContext): Unit = {}

  override def enterSql(ctx: SqlContext): Unit = {}

  override def enterFormat(ctx: FormatContext): Unit = {}

  override def exitFormat(ctx: FormatContext): Unit = {}

  override def enterPath(ctx: PathContext): Unit = {}

  override def exitPath(ctx: PathContext): Unit = {}

  override def enterTableName(ctx: TableNameContext): Unit = {}

  override def exitTableName(ctx: TableNameContext): Unit = {}

  override def enterCol(ctx: ColContext): Unit = {}

  override def exitCol(ctx: ColContext): Unit = {}

  override def enterQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def exitQualifiedName(ctx: QualifiedNameContext): Unit = {}

  override def enterIdentifier(ctx: IdentifierContext): Unit = {}

  override def exitIdentifier(ctx: IdentifierContext): Unit = {}

  override def enterStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def exitStrictIdentifier(ctx: StrictIdentifierContext): Unit = {}

  override def enterQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {}

  override def visitTerminal(node: TerminalNode): Unit = {}

  override def visitErrorNode(node: ErrorNode): Unit = {}

  override def exitEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}

  override def enterEnder(ctx: EnderContext): Unit = {}

  override def exitEnder(ctx: EnderContext): Unit = {}

  override def enterExpression(ctx: ExpressionContext): Unit = {}

  override def exitExpression(ctx: ExpressionContext): Unit = {}

  override def enterBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def exitBooleanExpression(ctx: BooleanExpressionContext): Unit = {}

  override def enterDb(ctx: DbContext): Unit = {}

  override def exitDb(ctx: DbContext): Unit = {}

  override def enterOverwrite(ctx: OverwriteContext): Unit = {}

  override def exitOverwrite(ctx: OverwriteContext): Unit = {}

  override def enterAppend(ctx: AppendContext): Unit = {}

  override def exitAppend(ctx: AppendContext): Unit = {}

  override def enterErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def exitErrorIfExists(ctx: ErrorIfExistsContext): Unit = {}

  override def enterIgnore(ctx: IgnoreContext): Unit = {}

  override def exitIgnore(ctx: IgnoreContext): Unit = {}

  override def enterFunctionName(ctx: FunctionNameContext): Unit = {}

  override def exitFunctionName(ctx: FunctionNameContext): Unit = {}

  override def enterSetValue(ctx: SetValueContext): Unit = {}

  override def exitSetValue(ctx: SetValueContext): Unit = {}

  override def enterSetKey(ctx: SetKeyContext): Unit = {}

  override def exitSetKey(ctx: SetKeyContext): Unit = {}

  override def enterAsTableName(ctx: AsTableNameContext): Unit = {}

  override def exitAsTableName(ctx: AsTableNameContext): Unit = {}

  override def enterColGroup(ctx: ColGroupContext): Unit = {}

  override def exitColGroup(ctx: ColGroupContext): Unit = {}
}
