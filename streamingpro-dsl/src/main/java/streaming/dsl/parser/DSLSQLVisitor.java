// Generated from /opt/projects/kyligence/byzerCP/byzer-lang/streamingpro-dsl/src/main/resources/DSLSQL.g4 by ANTLR 4.7

package streaming.dsl.parser;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link DSLSQLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface DSLSQLVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatement(DSLSQLParser.StatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#sql}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSql(DSLSQLParser.SqlContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#as}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAs(DSLSQLParser.AsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#into}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInto(DSLSQLParser.IntoContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#saveMode}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSaveMode(DSLSQLParser.SaveModeContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#where}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhere(DSLSQLParser.WhereContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#whereExpressions}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereExpressions(DSLSQLParser.WhereExpressionsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#overwrite}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOverwrite(DSLSQLParser.OverwriteContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#append}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAppend(DSLSQLParser.AppendContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#errorIfExists}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorIfExists(DSLSQLParser.ErrorIfExistsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#ignore}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIgnore(DSLSQLParser.IgnoreContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanExpression(DSLSQLParser.BooleanExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(DSLSQLParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#ender}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEnder(DSLSQLParser.EnderContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#format}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFormat(DSLSQLParser.FormatContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#path}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPath(DSLSQLParser.PathContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#commandValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommandValue(DSLSQLParser.CommandValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#rawCommandValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRawCommandValue(DSLSQLParser.RawCommandValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#setValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetValue(DSLSQLParser.SetValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#setKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetKey(DSLSQLParser.SetKeyContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#db}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDb(DSLSQLParser.DbContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#asTableName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAsTableName(DSLSQLParser.AsTableNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#tableName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(DSLSQLParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#functionName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionName(DSLSQLParser.FunctionNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#colGroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColGroup(DSLSQLParser.ColGroupContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#col}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCol(DSLSQLParser.ColContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(DSLSQLParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(DSLSQLParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStrictIdentifier(DSLSQLParser.StrictIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link DSLSQLParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(DSLSQLParser.QuotedIdentifierContext ctx);
}