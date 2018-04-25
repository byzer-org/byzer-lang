// Generated from DSLSQL.g4 by ANTLR 4.7.1

package streaming.dsl.parser;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DSLSQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, STRING=31, 
		IDENTIFIER=32, BACKQUOTED_IDENTIFIER=33, SIMPLE_COMMENT=34, BRACKETED_EMPTY_COMMENT=35, 
		BRACKETED_COMMENT=36, WS=37, UNRECOGNIZED=38;
	public static final int
		RULE_statement = 0, RULE_sql = 1, RULE_overwrite = 2, RULE_append = 3, 
		RULE_errorIfExists = 4, RULE_ignore = 5, RULE_booleanExpression = 6, RULE_expression = 7, 
		RULE_ender = 8, RULE_format = 9, RULE_path = 10, RULE_setValue = 11, RULE_setKey = 12, 
		RULE_db = 13, RULE_tableName = 14, RULE_functionName = 15, RULE_col = 16, 
		RULE_qualifiedName = 17, RULE_identifier = 18, RULE_strictIdentifier = 19, 
		RULE_quotedIdentifier = 20;
	public static final String[] ruleNames = {
		"statement", "sql", "overwrite", "append", "errorIfExists", "ignore", 
		"booleanExpression", "expression", "ender", "format", "path", "setValue", 
		"setKey", "db", "tableName", "functionName", "col", "qualifiedName", "identifier", 
		"strictIdentifier", "quotedIdentifier"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'LOAD'", "'.'", "'options'", "'as'", "'save'", "'SAVE'", 
		"'partitionBy'", "'select'", "'SELECT'", "';'", "'insert'", "'INSERT'", 
		"'create'", "'CREATE'", "'set'", "'SET'", "'='", "'connect'", "'CONNECT'", 
		"'where'", "'train'", "'TRAIN'", "'register'", "'REGISTER'", "'overwrite'", 
		"'append'", "'errorIfExists'", "'ignore'", "'and'", null, null, null, 
		null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", 
		"UNRECOGNIZED"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "DSLSQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public DSLSQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StatementContext extends ParserRuleContext {
		public List<SqlContext> sql() {
			return getRuleContexts(SqlContext.class);
		}
		public SqlContext sql(int i) {
			return getRuleContext(SqlContext.class,i);
		}
		public List<EnderContext> ender() {
			return getRuleContexts(EnderContext.class);
		}
		public EnderContext ender(int i) {
			return getRuleContext(EnderContext.class,i);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitStatement(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(47);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__5) | (1L << T__6) | (1L << T__8) | (1L << T__9) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__18) | (1L << T__19) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << SIMPLE_COMMENT))) != 0)) {
				{
				{
				setState(42);
				sql();
				setState(43);
				ender();
				}
				}
				setState(49);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SqlContext extends ParserRuleContext {
		public FormatContext format() {
			return getRuleContext(FormatContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public List<OverwriteContext> overwrite() {
			return getRuleContexts(OverwriteContext.class);
		}
		public OverwriteContext overwrite(int i) {
			return getRuleContext(OverwriteContext.class,i);
		}
		public List<AppendContext> append() {
			return getRuleContexts(AppendContext.class);
		}
		public AppendContext append(int i) {
			return getRuleContext(AppendContext.class,i);
		}
		public List<ErrorIfExistsContext> errorIfExists() {
			return getRuleContexts(ErrorIfExistsContext.class);
		}
		public ErrorIfExistsContext errorIfExists(int i) {
			return getRuleContext(ErrorIfExistsContext.class,i);
		}
		public List<IgnoreContext> ignore() {
			return getRuleContexts(IgnoreContext.class);
		}
		public IgnoreContext ignore(int i) {
			return getRuleContext(IgnoreContext.class,i);
		}
		public ColContext col() {
			return getRuleContext(ColContext.class,0);
		}
		public SetKeyContext setKey() {
			return getRuleContext(SetKeyContext.class,0);
		}
		public SetValueContext setValue() {
			return getRuleContext(SetValueContext.class,0);
		}
		public DbContext db() {
			return getRuleContext(DbContext.class,0);
		}
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode SIMPLE_COMMENT() { return getToken(DSLSQLParser.SIMPLE_COMMENT, 0); }
		public SqlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterSql(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitSql(this);
		}
	}

	public final SqlContext sql() throws RecognitionException {
		SqlContext _localctx = new SqlContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_sql);
		int _la;
		try {
			int _alt;
			setState(183);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(50);
				_la = _input.LA(1);
				if ( !(_la==T__0 || _la==T__1) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(51);
				format();
				setState(52);
				match(T__2);
				setState(53);
				path();
				setState(55);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(54);
					match(T__3);
					}
				}

				setState(58);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(57);
					expression();
					}
				}

				setState(63);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__29) {
					{
					{
					setState(60);
					booleanExpression();
					}
					}
					setState(65);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(66);
				match(T__4);
				setState(67);
				tableName();
				}
				break;
			case T__5:
			case T__6:
				enterOuterAlt(_localctx, 2);
				{
				setState(69);
				_la = _input.LA(1);
				if ( !(_la==T__5 || _la==T__6) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(76);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28))) != 0)) {
					{
					setState(74);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case T__25:
						{
						setState(70);
						overwrite();
						}
						break;
					case T__26:
						{
						setState(71);
						append();
						}
						break;
					case T__27:
						{
						setState(72);
						errorIfExists();
						}
						break;
					case T__28:
						{
						setState(73);
						ignore();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(78);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(79);
				tableName();
				setState(80);
				match(T__4);
				setState(81);
				format();
				setState(82);
				match(T__2);
				setState(83);
				path();
				setState(85);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(84);
					match(T__3);
					}
				}

				setState(88);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(87);
					expression();
					}
				}

				setState(93);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__29) {
					{
					{
					setState(90);
					booleanExpression();
					}
					}
					setState(95);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(98);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__7) {
					{
					setState(96);
					match(T__7);
					setState(97);
					col();
					}
				}

				}
				break;
			case T__8:
			case T__9:
				enterOuterAlt(_localctx, 3);
				{
				setState(100);
				_la = _input.LA(1);
				if ( !(_la==T__8 || _la==T__9) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(104);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(101);
						_la = _input.LA(1);
						if ( _la <= 0 || (_la==T__10) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						}
						} 
					}
					setState(106);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
				}
				setState(107);
				match(T__4);
				setState(108);
				tableName();
				}
				break;
			case T__11:
			case T__12:
				enterOuterAlt(_localctx, 4);
				{
				setState(109);
				_la = _input.LA(1);
				if ( !(_la==T__11 || _la==T__12) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(113);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29) | (1L << STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(110);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__10) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(115);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__13:
			case T__14:
				enterOuterAlt(_localctx, 5);
				{
				setState(116);
				_la = _input.LA(1);
				if ( !(_la==T__13 || _la==T__14) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29) | (1L << STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(117);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__10) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(122);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__15:
			case T__16:
				enterOuterAlt(_localctx, 6);
				{
				setState(123);
				_la = _input.LA(1);
				if ( !(_la==T__15 || _la==T__16) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(124);
				setKey();
				setState(125);
				match(T__17);
				setState(126);
				setValue();
				setState(128);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(127);
					match(T__3);
					}
				}

				setState(131);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(130);
					expression();
					}
				}

				setState(136);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__29) {
					{
					{
					setState(133);
					booleanExpression();
					}
					}
					setState(138);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__18:
			case T__19:
				enterOuterAlt(_localctx, 7);
				{
				setState(139);
				_la = _input.LA(1);
				if ( !(_la==T__18 || _la==T__19) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(140);
				format();
				setState(142);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__20) {
					{
					setState(141);
					match(T__20);
					}
				}

				setState(145);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(144);
					expression();
					}
				}

				setState(150);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__29) {
					{
					{
					setState(147);
					booleanExpression();
					}
					}
					setState(152);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(155);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__4) {
					{
					setState(153);
					match(T__4);
					setState(154);
					db();
					}
				}

				}
				break;
			case T__21:
			case T__22:
				enterOuterAlt(_localctx, 8);
				{
				setState(157);
				_la = _input.LA(1);
				if ( !(_la==T__21 || _la==T__22) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(158);
				tableName();
				setState(159);
				match(T__4);
				setState(160);
				format();
				setState(161);
				match(T__2);
				setState(162);
				path();
				setState(164);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__20) {
					{
					setState(163);
					match(T__20);
					}
				}

				setState(167);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(166);
					expression();
					}
				}

				setState(172);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__29) {
					{
					{
					setState(169);
					booleanExpression();
					}
					}
					setState(174);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__23:
			case T__24:
				enterOuterAlt(_localctx, 9);
				{
				setState(175);
				_la = _input.LA(1);
				if ( !(_la==T__23 || _la==T__24) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(176);
				format();
				setState(177);
				match(T__2);
				setState(178);
				path();
				setState(179);
				match(T__4);
				setState(180);
				functionName();
				}
				break;
			case SIMPLE_COMMENT:
				enterOuterAlt(_localctx, 10);
				{
				setState(182);
				match(SIMPLE_COMMENT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OverwriteContext extends ParserRuleContext {
		public OverwriteContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_overwrite; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterOverwrite(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitOverwrite(this);
		}
	}

	public final OverwriteContext overwrite() throws RecognitionException {
		OverwriteContext _localctx = new OverwriteContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_overwrite);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(185);
			match(T__25);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AppendContext extends ParserRuleContext {
		public AppendContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_append; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterAppend(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitAppend(this);
		}
	}

	public final AppendContext append() throws RecognitionException {
		AppendContext _localctx = new AppendContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_append);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(187);
			match(T__26);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ErrorIfExistsContext extends ParserRuleContext {
		public ErrorIfExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_errorIfExists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterErrorIfExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitErrorIfExists(this);
		}
	}

	public final ErrorIfExistsContext errorIfExists() throws RecognitionException {
		ErrorIfExistsContext _localctx = new ErrorIfExistsContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_errorIfExists);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(189);
			match(T__27);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IgnoreContext extends ParserRuleContext {
		public IgnoreContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ignore; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterIgnore(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitIgnore(this);
		}
	}

	public final IgnoreContext ignore() throws RecognitionException {
		IgnoreContext _localctx = new IgnoreContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_ignore);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(191);
			match(T__28);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterBooleanExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitBooleanExpression(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_booleanExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(193);
			match(T__29);
			setState(194);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitExpression(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(196);
			identifier();
			setState(197);
			match(T__17);
			setState(198);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EnderContext extends ParserRuleContext {
		public EnderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ender; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterEnder(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitEnder(this);
		}
	}

	public final EnderContext ender() throws RecognitionException {
		EnderContext _localctx = new EnderContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_ender);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(200);
			match(T__10);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FormatContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public FormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_format; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitFormat(this);
		}
	}

	public final FormatContext format() throws RecognitionException {
		FormatContext _localctx = new FormatContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_format);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(202);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitPath(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_path);
		try {
			setState(206);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(204);
				quotedIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(205);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetValueContext extends ParserRuleContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public SetValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterSetValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitSetValue(this);
		}
	}

	public final SetValueContext setValue() throws RecognitionException {
		SetValueContext _localctx = new SetValueContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_setValue);
		try {
			setState(210);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(208);
				quotedIdentifier();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(209);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetKeyContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public SetKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterSetKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitSetKey(this);
		}
	}

	public final SetKeyContext setKey() throws RecognitionException {
		SetKeyContext _localctx = new SetKeyContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_setKey);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(212);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DbContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DbContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_db; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterDb(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitDb(this);
		}
	}

	public final DbContext db() throws RecognitionException {
		DbContext _localctx = new DbContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_db);
		try {
			setState(216);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(214);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(215);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitTableName(this);
		}
	}

	public final TableNameContext tableName() throws RecognitionException {
		TableNameContext _localctx = new TableNameContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_tableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(218);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionNameContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitFunctionName(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_functionName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(220);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_col; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterCol(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitCol(this);
		}
	}

	public final ColContext col() throws RecognitionException {
		ColContext _localctx = new ColContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_col);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(222);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitQualifiedName(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_qualifiedName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(224);
			identifier();
			setState(229);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(225);
				match(T__2);
				setState(226);
				identifier();
				}
				}
				setState(231);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitIdentifier(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(232);
			strictIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(DSLSQLParser.IDENTIFIER, 0); }
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterStrictIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitStrictIdentifier(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_strictIdentifier);
		try {
			setState(236);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(234);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(235);
				quotedIdentifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(DSLSQLParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitQuotedIdentifier(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(238);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3(\u00f3\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\3\2\3\2\7\2\60\n\2\f\2\16"+
		"\2\63\13\2\3\3\3\3\3\3\3\3\3\3\5\3:\n\3\3\3\5\3=\n\3\3\3\7\3@\n\3\f\3"+
		"\16\3C\13\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3M\n\3\f\3\16\3P\13\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\5\3X\n\3\3\3\5\3[\n\3\3\3\7\3^\n\3\f\3\16\3a\13"+
		"\3\3\3\3\3\5\3e\n\3\3\3\3\3\7\3i\n\3\f\3\16\3l\13\3\3\3\3\3\3\3\3\3\7"+
		"\3r\n\3\f\3\16\3u\13\3\3\3\3\3\7\3y\n\3\f\3\16\3|\13\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3\u0083\n\3\3\3\5\3\u0086\n\3\3\3\7\3\u0089\n\3\f\3\16\3\u008c"+
		"\13\3\3\3\3\3\3\3\5\3\u0091\n\3\3\3\5\3\u0094\n\3\3\3\7\3\u0097\n\3\f"+
		"\3\16\3\u009a\13\3\3\3\3\3\5\3\u009e\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5"+
		"\3\u00a7\n\3\3\3\5\3\u00aa\n\3\3\3\7\3\u00ad\n\3\f\3\16\3\u00b0\13\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\u00ba\n\3\3\4\3\4\3\5\3\5\3\6\3\6\3"+
		"\7\3\7\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\5\f\u00d1"+
		"\n\f\3\r\3\r\5\r\u00d5\n\r\3\16\3\16\3\17\3\17\5\17\u00db\n\17\3\20\3"+
		"\20\3\21\3\21\3\22\3\22\3\23\3\23\3\23\7\23\u00e6\n\23\f\23\16\23\u00e9"+
		"\13\23\3\24\3\24\3\25\3\25\5\25\u00ef\n\25\3\26\3\26\3\26\2\2\27\2\4\6"+
		"\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*\2\f\3\2\3\4\3\2\b\t\3\2\13\f"+
		"\3\2\r\r\3\2\16\17\3\2\20\21\3\2\22\23\3\2\25\26\3\2\30\31\3\2\32\33\2"+
		"\u0104\2\61\3\2\2\2\4\u00b9\3\2\2\2\6\u00bb\3\2\2\2\b\u00bd\3\2\2\2\n"+
		"\u00bf\3\2\2\2\f\u00c1\3\2\2\2\16\u00c3\3\2\2\2\20\u00c6\3\2\2\2\22\u00ca"+
		"\3\2\2\2\24\u00cc\3\2\2\2\26\u00d0\3\2\2\2\30\u00d4\3\2\2\2\32\u00d6\3"+
		"\2\2\2\34\u00da\3\2\2\2\36\u00dc\3\2\2\2 \u00de\3\2\2\2\"\u00e0\3\2\2"+
		"\2$\u00e2\3\2\2\2&\u00ea\3\2\2\2(\u00ee\3\2\2\2*\u00f0\3\2\2\2,-\5\4\3"+
		"\2-.\5\22\n\2.\60\3\2\2\2/,\3\2\2\2\60\63\3\2\2\2\61/\3\2\2\2\61\62\3"+
		"\2\2\2\62\3\3\2\2\2\63\61\3\2\2\2\64\65\t\2\2\2\65\66\5\24\13\2\66\67"+
		"\7\5\2\2\679\5\26\f\28:\7\6\2\298\3\2\2\29:\3\2\2\2:<\3\2\2\2;=\5\20\t"+
		"\2<;\3\2\2\2<=\3\2\2\2=A\3\2\2\2>@\5\16\b\2?>\3\2\2\2@C\3\2\2\2A?\3\2"+
		"\2\2AB\3\2\2\2BD\3\2\2\2CA\3\2\2\2DE\7\7\2\2EF\5\36\20\2F\u00ba\3\2\2"+
		"\2GN\t\3\2\2HM\5\6\4\2IM\5\b\5\2JM\5\n\6\2KM\5\f\7\2LH\3\2\2\2LI\3\2\2"+
		"\2LJ\3\2\2\2LK\3\2\2\2MP\3\2\2\2NL\3\2\2\2NO\3\2\2\2OQ\3\2\2\2PN\3\2\2"+
		"\2QR\5\36\20\2RS\7\7\2\2ST\5\24\13\2TU\7\5\2\2UW\5\26\f\2VX\7\6\2\2WV"+
		"\3\2\2\2WX\3\2\2\2XZ\3\2\2\2Y[\5\20\t\2ZY\3\2\2\2Z[\3\2\2\2[_\3\2\2\2"+
		"\\^\5\16\b\2]\\\3\2\2\2^a\3\2\2\2_]\3\2\2\2_`\3\2\2\2`d\3\2\2\2a_\3\2"+
		"\2\2bc\7\n\2\2ce\5\"\22\2db\3\2\2\2de\3\2\2\2e\u00ba\3\2\2\2fj\t\4\2\2"+
		"gi\n\5\2\2hg\3\2\2\2il\3\2\2\2jh\3\2\2\2jk\3\2\2\2km\3\2\2\2lj\3\2\2\2"+
		"mn\7\7\2\2n\u00ba\5\36\20\2os\t\6\2\2pr\n\5\2\2qp\3\2\2\2ru\3\2\2\2sq"+
		"\3\2\2\2st\3\2\2\2t\u00ba\3\2\2\2us\3\2\2\2vz\t\7\2\2wy\n\5\2\2xw\3\2"+
		"\2\2y|\3\2\2\2zx\3\2\2\2z{\3\2\2\2{\u00ba\3\2\2\2|z\3\2\2\2}~\t\b\2\2"+
		"~\177\5\32\16\2\177\u0080\7\24\2\2\u0080\u0082\5\30\r\2\u0081\u0083\7"+
		"\6\2\2\u0082\u0081\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u0085\3\2\2\2\u0084"+
		"\u0086\5\20\t\2\u0085\u0084\3\2\2\2\u0085\u0086\3\2\2\2\u0086\u008a\3"+
		"\2\2\2\u0087\u0089\5\16\b\2\u0088\u0087\3\2\2\2\u0089\u008c\3\2\2\2\u008a"+
		"\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b\u00ba\3\2\2\2\u008c\u008a\3\2"+
		"\2\2\u008d\u008e\t\t\2\2\u008e\u0090\5\24\13\2\u008f\u0091\7\27\2\2\u0090"+
		"\u008f\3\2\2\2\u0090\u0091\3\2\2\2\u0091\u0093\3\2\2\2\u0092\u0094\5\20"+
		"\t\2\u0093\u0092\3\2\2\2\u0093\u0094\3\2\2\2\u0094\u0098\3\2\2\2\u0095"+
		"\u0097\5\16\b\2\u0096\u0095\3\2\2\2\u0097\u009a\3\2\2\2\u0098\u0096\3"+
		"\2\2\2\u0098\u0099\3\2\2\2\u0099\u009d\3\2\2\2\u009a\u0098\3\2\2\2\u009b"+
		"\u009c\7\7\2\2\u009c\u009e\5\34\17\2\u009d\u009b\3\2\2\2\u009d\u009e\3"+
		"\2\2\2\u009e\u00ba\3\2\2\2\u009f\u00a0\t\n\2\2\u00a0\u00a1\5\36\20\2\u00a1"+
		"\u00a2\7\7\2\2\u00a2\u00a3\5\24\13\2\u00a3\u00a4\7\5\2\2\u00a4\u00a6\5"+
		"\26\f\2\u00a5\u00a7\7\27\2\2\u00a6\u00a5\3\2\2\2\u00a6\u00a7\3\2\2\2\u00a7"+
		"\u00a9\3\2\2\2\u00a8\u00aa\5\20\t\2\u00a9\u00a8\3\2\2\2\u00a9\u00aa\3"+
		"\2\2\2\u00aa\u00ae\3\2\2\2\u00ab\u00ad\5\16\b\2\u00ac\u00ab\3\2\2\2\u00ad"+
		"\u00b0\3\2\2\2\u00ae\u00ac\3\2\2\2\u00ae\u00af\3\2\2\2\u00af\u00ba\3\2"+
		"\2\2\u00b0\u00ae\3\2\2\2\u00b1\u00b2\t\13\2\2\u00b2\u00b3\5\24\13\2\u00b3"+
		"\u00b4\7\5\2\2\u00b4\u00b5\5\26\f\2\u00b5\u00b6\7\7\2\2\u00b6\u00b7\5"+
		" \21\2\u00b7\u00ba\3\2\2\2\u00b8\u00ba\7$\2\2\u00b9\64\3\2\2\2\u00b9G"+
		"\3\2\2\2\u00b9f\3\2\2\2\u00b9o\3\2\2\2\u00b9v\3\2\2\2\u00b9}\3\2\2\2\u00b9"+
		"\u008d\3\2\2\2\u00b9\u009f\3\2\2\2\u00b9\u00b1\3\2\2\2\u00b9\u00b8\3\2"+
		"\2\2\u00ba\5\3\2\2\2\u00bb\u00bc\7\34\2\2\u00bc\7\3\2\2\2\u00bd\u00be"+
		"\7\35\2\2\u00be\t\3\2\2\2\u00bf\u00c0\7\36\2\2\u00c0\13\3\2\2\2\u00c1"+
		"\u00c2\7\37\2\2\u00c2\r\3\2\2\2\u00c3\u00c4\7 \2\2\u00c4\u00c5\5\20\t"+
		"\2\u00c5\17\3\2\2\2\u00c6\u00c7\5&\24\2\u00c7\u00c8\7\24\2\2\u00c8\u00c9"+
		"\7!\2\2\u00c9\21\3\2\2\2\u00ca\u00cb\7\r\2\2\u00cb\23\3\2\2\2\u00cc\u00cd"+
		"\5&\24\2\u00cd\25\3\2\2\2\u00ce\u00d1\5*\26\2\u00cf\u00d1\5&\24\2\u00d0"+
		"\u00ce\3\2\2\2\u00d0\u00cf\3\2\2\2\u00d1\27\3\2\2\2\u00d2\u00d5\5*\26"+
		"\2\u00d3\u00d5\7!\2\2\u00d4\u00d2\3\2\2\2\u00d4\u00d3\3\2\2\2\u00d5\31"+
		"\3\2\2\2\u00d6\u00d7\5&\24\2\u00d7\33\3\2\2\2\u00d8\u00db\5$\23\2\u00d9"+
		"\u00db\5&\24\2\u00da\u00d8\3\2\2\2\u00da\u00d9\3\2\2\2\u00db\35\3\2\2"+
		"\2\u00dc\u00dd\5&\24\2\u00dd\37\3\2\2\2\u00de\u00df\5&\24\2\u00df!\3\2"+
		"\2\2\u00e0\u00e1\5&\24\2\u00e1#\3\2\2\2\u00e2\u00e7\5&\24\2\u00e3\u00e4"+
		"\7\5\2\2\u00e4\u00e6\5&\24\2\u00e5\u00e3\3\2\2\2\u00e6\u00e9\3\2\2\2\u00e7"+
		"\u00e5\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8%\3\2\2\2\u00e9\u00e7\3\2\2\2"+
		"\u00ea\u00eb\5(\25\2\u00eb\'\3\2\2\2\u00ec\u00ef\7\"\2\2\u00ed\u00ef\5"+
		"*\26\2\u00ee\u00ec\3\2\2\2\u00ee\u00ed\3\2\2\2\u00ef)\3\2\2\2\u00f0\u00f1"+
		"\7#\2\2\u00f1+\3\2\2\2\37\619<ALNWZ_djsz\u0082\u0085\u008a\u0090\u0093"+
		"\u0098\u009d\u00a6\u00a9\u00ae\u00b9\u00d0\u00d4\u00da\u00e7\u00ee";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}