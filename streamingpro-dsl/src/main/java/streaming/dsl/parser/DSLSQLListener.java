// Generated from /opt/projects/kyligence/byzerCP/byzer-lang/streamingpro-dsl/src/main/resources/DSLSQL.g4 by ANTLR 4.7

package streaming.dsl.parser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link DSLSQLParser}.
 */
public interface DSLSQLListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatement(DSLSQLParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatement(DSLSQLParser.StatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#sql}.
	 * @param ctx the parse tree
	 */
	void enterSql(DSLSQLParser.SqlContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#sql}.
	 * @param ctx the parse tree
	 */
	void exitSql(DSLSQLParser.SqlContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#as}.
	 * @param ctx the parse tree
	 */
	void enterAs(DSLSQLParser.AsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#as}.
	 * @param ctx the parse tree
	 */
	void exitAs(DSLSQLParser.AsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#into}.
	 * @param ctx the parse tree
	 */
	void enterInto(DSLSQLParser.IntoContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#into}.
	 * @param ctx the parse tree
	 */
	void exitInto(DSLSQLParser.IntoContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#saveMode}.
	 * @param ctx the parse tree
	 */
	void enterSaveMode(DSLSQLParser.SaveModeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#saveMode}.
	 * @param ctx the parse tree
	 */
	void exitSaveMode(DSLSQLParser.SaveModeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#where}.
	 * @param ctx the parse tree
	 */
	void enterWhere(DSLSQLParser.WhereContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#where}.
	 * @param ctx the parse tree
	 */
	void exitWhere(DSLSQLParser.WhereContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#whereExpressions}.
	 * @param ctx the parse tree
	 */
	void enterWhereExpressions(DSLSQLParser.WhereExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#whereExpressions}.
	 * @param ctx the parse tree
	 */
	void exitWhereExpressions(DSLSQLParser.WhereExpressionsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#overwrite}.
	 * @param ctx the parse tree
	 */
	void enterOverwrite(DSLSQLParser.OverwriteContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#overwrite}.
	 * @param ctx the parse tree
	 */
	void exitOverwrite(DSLSQLParser.OverwriteContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#append}.
	 * @param ctx the parse tree
	 */
	void enterAppend(DSLSQLParser.AppendContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#append}.
	 * @param ctx the parse tree
	 */
	void exitAppend(DSLSQLParser.AppendContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#errorIfExists}.
	 * @param ctx the parse tree
	 */
	void enterErrorIfExists(DSLSQLParser.ErrorIfExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#errorIfExists}.
	 * @param ctx the parse tree
	 */
	void exitErrorIfExists(DSLSQLParser.ErrorIfExistsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#ignore}.
	 * @param ctx the parse tree
	 */
	void enterIgnore(DSLSQLParser.IgnoreContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#ignore}.
	 * @param ctx the parse tree
	 */
	void exitIgnore(DSLSQLParser.IgnoreContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanExpression(DSLSQLParser.BooleanExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanExpression(DSLSQLParser.BooleanExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(DSLSQLParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(DSLSQLParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#ender}.
	 * @param ctx the parse tree
	 */
	void enterEnder(DSLSQLParser.EnderContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#ender}.
	 * @param ctx the parse tree
	 */
	void exitEnder(DSLSQLParser.EnderContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#format}.
	 * @param ctx the parse tree
	 */
	void enterFormat(DSLSQLParser.FormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#format}.
	 * @param ctx the parse tree
	 */
	void exitFormat(DSLSQLParser.FormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#path}.
	 * @param ctx the parse tree
	 */
	void enterPath(DSLSQLParser.PathContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#path}.
	 * @param ctx the parse tree
	 */
	void exitPath(DSLSQLParser.PathContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#commandValue}.
	 * @param ctx the parse tree
	 */
	void enterCommandValue(DSLSQLParser.CommandValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#commandValue}.
	 * @param ctx the parse tree
	 */
	void exitCommandValue(DSLSQLParser.CommandValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#rawCommandValue}.
	 * @param ctx the parse tree
	 */
	void enterRawCommandValue(DSLSQLParser.RawCommandValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#rawCommandValue}.
	 * @param ctx the parse tree
	 */
	void exitRawCommandValue(DSLSQLParser.RawCommandValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#setValue}.
	 * @param ctx the parse tree
	 */
	void enterSetValue(DSLSQLParser.SetValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#setValue}.
	 * @param ctx the parse tree
	 */
	void exitSetValue(DSLSQLParser.SetValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#setKey}.
	 * @param ctx the parse tree
	 */
	void enterSetKey(DSLSQLParser.SetKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#setKey}.
	 * @param ctx the parse tree
	 */
	void exitSetKey(DSLSQLParser.SetKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#db}.
	 * @param ctx the parse tree
	 */
	void enterDb(DSLSQLParser.DbContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#db}.
	 * @param ctx the parse tree
	 */
	void exitDb(DSLSQLParser.DbContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#asTableName}.
	 * @param ctx the parse tree
	 */
	void enterAsTableName(DSLSQLParser.AsTableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#asTableName}.
	 * @param ctx the parse tree
	 */
	void exitAsTableName(DSLSQLParser.AsTableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#tableName}.
	 * @param ctx the parse tree
	 */
	void enterTableName(DSLSQLParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#tableName}.
	 * @param ctx the parse tree
	 */
	void exitTableName(DSLSQLParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#functionName}.
	 * @param ctx the parse tree
	 */
	void enterFunctionName(DSLSQLParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#functionName}.
	 * @param ctx the parse tree
	 */
	void exitFunctionName(DSLSQLParser.FunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#colGroup}.
	 * @param ctx the parse tree
	 */
	void enterColGroup(DSLSQLParser.ColGroupContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#colGroup}.
	 * @param ctx the parse tree
	 */
	void exitColGroup(DSLSQLParser.ColGroupContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#col}.
	 * @param ctx the parse tree
	 */
	void enterCol(DSLSQLParser.ColContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#col}.
	 * @param ctx the parse tree
	 */
	void exitCol(DSLSQLParser.ColContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(DSLSQLParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(DSLSQLParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(DSLSQLParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(DSLSQLParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterStrictIdentifier(DSLSQLParser.StrictIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitStrictIdentifier(DSLSQLParser.StrictIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link DSLSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(DSLSQLParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link DSLSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(DSLSQLParser.QuotedIdentifierContext ctx);
}