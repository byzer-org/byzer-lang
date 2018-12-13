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
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, STRING=30, BLOCK_STRING=31, 
		IDENTIFIER=32, BACKQUOTED_IDENTIFIER=33, SIMPLE_COMMENT=34, BRACKETED_EMPTY_COMMENT=35, 
		BRACKETED_COMMENT=36, WS=37, UNRECOGNIZED=38;
	public static final int
		RULE_statement = 0, RULE_sql = 1, RULE_overwrite = 2, RULE_append = 3, 
		RULE_errorIfExists = 4, RULE_ignore = 5, RULE_booleanExpression = 6, RULE_expression = 7, 
		RULE_ender = 8, RULE_format = 9, RULE_path = 10, RULE_setValue = 11, RULE_setKey = 12, 
		RULE_db = 13, RULE_asTableName = 14, RULE_tableName = 15, RULE_functionName = 16, 
		RULE_colGroup = 17, RULE_col = 18, RULE_qualifiedName = 19, RULE_identifier = 20, 
		RULE_strictIdentifier = 21, RULE_quotedIdentifier = 22;
	public static final String[] ruleNames = {
		"statement", "sql", "overwrite", "append", "errorIfExists", "ignore", 
		"booleanExpression", "expression", "ender", "format", "path", "setValue", 
		"setKey", "db", "asTableName", "tableName", "functionName", "colGroup", 
		"col", "qualifiedName", "identifier", "strictIdentifier", "quotedIdentifier"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'options'", "'where'", "'as'", "'save'", "'partitionBy'", 
		"'partitionby'", "'select'", "';'", "'insert'", "'create'", "'drop'", 
		"'refresh'", "'set'", "'='", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'overwrite'", "'append'", 
		"'errorIfExists'", "'ignore'", "'and'", "','", null, null, null, null, 
		null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, "STRING", "BLOCK_STRING", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
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
			setState(51);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__5) | (1L << T__8) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << SIMPLE_COMMENT))) != 0)) {
				{
				{
				setState(46);
				sql();
				setState(47);
				ender();
				}
				}
				setState(53);
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
		public List<ColGroupContext> colGroup() {
			return getRuleContexts(ColGroupContext.class);
		}
		public ColGroupContext colGroup(int i) {
			return getRuleContext(ColGroupContext.class,i);
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
		public List<AsTableNameContext> asTableName() {
			return getRuleContexts(AsTableNameContext.class);
		}
		public AsTableNameContext asTableName(int i) {
			return getRuleContext(AsTableNameContext.class,i);
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
			setState(258);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
				enterOuterAlt(_localctx, 1);
				{
				setState(54);
				match(T__0);
				setState(55);
				format();
				setState(56);
				match(T__1);
				setState(57);
				path();
				setState(59);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(58);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(62);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(61);
					expression();
					}
				}

				setState(67);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(64);
					booleanExpression();
					}
					}
					setState(69);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(70);
				match(T__4);
				setState(71);
				tableName();
				}
				break;
			case T__5:
				enterOuterAlt(_localctx, 2);
				{
				setState(73);
				match(T__5);
				setState(80);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26))) != 0)) {
					{
					setState(78);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case T__23:
						{
						setState(74);
						overwrite();
						}
						break;
					case T__24:
						{
						setState(75);
						append();
						}
						break;
					case T__25:
						{
						setState(76);
						errorIfExists();
						}
						break;
					case T__26:
						{
						setState(77);
						ignore();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					setState(82);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(83);
				tableName();
				setState(84);
				match(T__4);
				setState(85);
				format();
				setState(86);
				match(T__1);
				setState(87);
				path();
				setState(89);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(88);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(92);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(91);
					expression();
					}
				}

				setState(97);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(94);
					booleanExpression();
					}
					}
					setState(99);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(110);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__6 || _la==T__7) {
					{
					setState(100);
					_la = _input.LA(1);
					if ( !(_la==T__6 || _la==T__7) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(102);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
						{
						setState(101);
						col();
						}
					}

					setState(107);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__28) {
						{
						{
						setState(104);
						colGroup();
						}
						}
						setState(109);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 3);
				{
				setState(112);
				match(T__8);
				setState(116);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(113);
						_la = _input.LA(1);
						if ( _la <= 0 || (_la==T__9) ) {
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
					setState(118);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,12,_ctx);
				}
				setState(119);
				match(T__4);
				setState(120);
				tableName();
				}
				break;
			case T__10:
				enterOuterAlt(_localctx, 4);
				{
				setState(121);
				match(T__10);
				setState(125);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(122);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__9) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(127);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__11:
				enterOuterAlt(_localctx, 5);
				{
				setState(128);
				match(T__11);
				setState(132);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(129);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__9) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(134);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__12:
				enterOuterAlt(_localctx, 6);
				{
				setState(135);
				match(T__12);
				setState(139);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(136);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__9) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(141);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__13:
				enterOuterAlt(_localctx, 7);
				{
				setState(142);
				match(T__13);
				setState(146);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19) | (1L << T__20) | (1L << T__21) | (1L << T__22) | (1L << T__23) | (1L << T__24) | (1L << T__25) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << STRING) | (1L << BLOCK_STRING) | (1L << IDENTIFIER) | (1L << BACKQUOTED_IDENTIFIER) | (1L << SIMPLE_COMMENT) | (1L << BRACKETED_EMPTY_COMMENT) | (1L << BRACKETED_COMMENT) | (1L << WS) | (1L << UNRECOGNIZED))) != 0)) {
					{
					{
					setState(143);
					_la = _input.LA(1);
					if ( _la <= 0 || (_la==T__9) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(148);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__14:
				enterOuterAlt(_localctx, 8);
				{
				setState(149);
				match(T__14);
				setState(150);
				setKey();
				setState(151);
				match(T__15);
				setState(152);
				setValue();
				setState(154);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(153);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(157);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(156);
					expression();
					}
				}

				setState(162);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(159);
					booleanExpression();
					}
					}
					setState(164);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__16:
				enterOuterAlt(_localctx, 9);
				{
				setState(165);
				match(T__16);
				setState(166);
				format();
				setState(168);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(167);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(171);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(170);
					expression();
					}
				}

				setState(176);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(173);
					booleanExpression();
					}
					}
					setState(178);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(181);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__4) {
					{
					setState(179);
					match(T__4);
					setState(180);
					db();
					}
				}

				}
				break;
			case T__17:
			case T__18:
			case T__19:
				enterOuterAlt(_localctx, 10);
				{
				setState(183);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__17) | (1L << T__18) | (1L << T__19))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(184);
				tableName();
				setState(185);
				match(T__4);
				setState(186);
				format();
				setState(187);
				match(T__1);
				setState(188);
				path();
				setState(190);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(189);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(193);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(192);
					expression();
					}
				}

				setState(198);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(195);
					booleanExpression();
					}
					}
					setState(200);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(204);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__4) {
					{
					{
					setState(201);
					asTableName();
					}
					}
					setState(206);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__20:
				enterOuterAlt(_localctx, 11);
				{
				setState(207);
				match(T__20);
				setState(208);
				format();
				setState(209);
				match(T__1);
				setState(210);
				path();
				setState(211);
				match(T__4);
				setState(212);
				functionName();
				setState(214);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(213);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(217);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(216);
					expression();
					}
				}

				setState(222);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(219);
					booleanExpression();
					}
					}
					setState(224);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__21:
				enterOuterAlt(_localctx, 12);
				{
				setState(225);
				match(T__21);
				setState(226);
				format();
				setState(227);
				match(T__1);
				setState(228);
				path();
				setState(230);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(229);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(232);
					expression();
					}
				}

				setState(238);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(235);
					booleanExpression();
					}
					}
					setState(240);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case T__22:
				enterOuterAlt(_localctx, 13);
				{
				setState(241);
				match(T__22);
				setState(242);
				format();
				setState(243);
				match(T__1);
				setState(244);
				path();
				setState(246);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2 || _la==T__3) {
					{
					setState(245);
					_la = _input.LA(1);
					if ( !(_la==T__2 || _la==T__3) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
				}

				setState(249);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==IDENTIFIER || _la==BACKQUOTED_IDENTIFIER) {
					{
					setState(248);
					expression();
					}
				}

				setState(254);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__27) {
					{
					{
					setState(251);
					booleanExpression();
					}
					}
					setState(256);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case SIMPLE_COMMENT:
				enterOuterAlt(_localctx, 14);
				{
				setState(257);
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
			setState(260);
			match(T__23);
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
			setState(262);
			match(T__24);
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
			setState(264);
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
			setState(266);
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
			setState(268);
			match(T__27);
			setState(269);
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
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public TerminalNode BLOCK_STRING() { return getToken(DSLSQLParser.BLOCK_STRING, 0); }
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
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(271);
			qualifiedName();
			setState(272);
			match(T__15);
			setState(273);
			_la = _input.LA(1);
			if ( !(_la==STRING || _la==BLOCK_STRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
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
			setState(275);
			match(T__9);
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
			setState(277);
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
			setState(281);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,38,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(279);
				quotedIdentifier();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(280);
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
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(DSLSQLParser.STRING, 0); }
		public TerminalNode BLOCK_STRING() { return getToken(DSLSQLParser.BLOCK_STRING, 0); }
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
			setState(287);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(283);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(284);
				quotedIdentifier();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(285);
				match(STRING);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(286);
				match(BLOCK_STRING);
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

	public static class SetKeyContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
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
			setState(289);
			qualifiedName();
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
			setState(293);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(291);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(292);
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

	public static class AsTableNameContext extends ParserRuleContext {
		public TableNameContext tableName() {
			return getRuleContext(TableNameContext.class,0);
		}
		public AsTableNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_asTableName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterAsTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitAsTableName(this);
		}
	}

	public final AsTableNameContext asTableName() throws RecognitionException {
		AsTableNameContext _localctx = new AsTableNameContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_asTableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(295);
			match(T__4);
			setState(296);
			tableName();
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
		enterRule(_localctx, 30, RULE_tableName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(298);
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
		enterRule(_localctx, 32, RULE_functionName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(300);
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

	public static class ColGroupContext extends ParserRuleContext {
		public ColContext col() {
			return getRuleContext(ColContext.class,0);
		}
		public ColGroupContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colGroup; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).enterColGroup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DSLSQLListener ) ((DSLSQLListener)listener).exitColGroup(this);
		}
	}

	public final ColGroupContext colGroup() throws RecognitionException {
		ColGroupContext _localctx = new ColGroupContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_colGroup);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(302);
			match(T__28);
			setState(303);
			col();
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
		enterRule(_localctx, 36, RULE_col);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(305);
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
		enterRule(_localctx, 38, RULE_qualifiedName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(307);
			identifier();
			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(308);
				match(T__1);
				setState(309);
				identifier();
				}
				}
				setState(314);
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
		enterRule(_localctx, 40, RULE_identifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(315);
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
		enterRule(_localctx, 42, RULE_strictIdentifier);
		try {
			setState(319);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(317);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(318);
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
		enterRule(_localctx, 44, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(321);
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3(\u0146\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\3\2\3\2\3"+
		"\2\7\2\64\n\2\f\2\16\2\67\13\2\3\3\3\3\3\3\3\3\3\3\5\3>\n\3\3\3\5\3A\n"+
		"\3\3\3\7\3D\n\3\f\3\16\3G\13\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\7\3Q\n"+
		"\3\f\3\16\3T\13\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\\\n\3\3\3\5\3_\n\3\3\3\7"+
		"\3b\n\3\f\3\16\3e\13\3\3\3\3\3\5\3i\n\3\3\3\7\3l\n\3\f\3\16\3o\13\3\5"+
		"\3q\n\3\3\3\3\3\7\3u\n\3\f\3\16\3x\13\3\3\3\3\3\3\3\3\3\7\3~\n\3\f\3\16"+
		"\3\u0081\13\3\3\3\3\3\7\3\u0085\n\3\f\3\16\3\u0088\13\3\3\3\3\3\7\3\u008c"+
		"\n\3\f\3\16\3\u008f\13\3\3\3\3\3\7\3\u0093\n\3\f\3\16\3\u0096\13\3\3\3"+
		"\3\3\3\3\3\3\3\3\5\3\u009d\n\3\3\3\5\3\u00a0\n\3\3\3\7\3\u00a3\n\3\f\3"+
		"\16\3\u00a6\13\3\3\3\3\3\3\3\5\3\u00ab\n\3\3\3\5\3\u00ae\n\3\3\3\7\3\u00b1"+
		"\n\3\f\3\16\3\u00b4\13\3\3\3\3\3\5\3\u00b8\n\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\5\3\u00c1\n\3\3\3\5\3\u00c4\n\3\3\3\7\3\u00c7\n\3\f\3\16\3\u00ca"+
		"\13\3\3\3\7\3\u00cd\n\3\f\3\16\3\u00d0\13\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\5\3\u00d9\n\3\3\3\5\3\u00dc\n\3\3\3\7\3\u00df\n\3\f\3\16\3\u00e2\13"+
		"\3\3\3\3\3\3\3\3\3\3\3\5\3\u00e9\n\3\3\3\5\3\u00ec\n\3\3\3\7\3\u00ef\n"+
		"\3\f\3\16\3\u00f2\13\3\3\3\3\3\3\3\3\3\3\3\5\3\u00f9\n\3\3\3\5\3\u00fc"+
		"\n\3\3\3\7\3\u00ff\n\3\f\3\16\3\u0102\13\3\3\3\5\3\u0105\n\3\3\4\3\4\3"+
		"\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\n\3\n\3\13\3\13\3"+
		"\f\3\f\5\f\u011c\n\f\3\r\3\r\3\r\3\r\5\r\u0122\n\r\3\16\3\16\3\17\3\17"+
		"\5\17\u0128\n\17\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\23\3\23\3\23\3\24"+
		"\3\24\3\25\3\25\3\25\7\25\u0139\n\25\f\25\16\25\u013c\13\25\3\26\3\26"+
		"\3\27\3\27\5\27\u0142\n\27\3\30\3\30\3\30\2\2\31\2\4\6\b\n\f\16\20\22"+
		"\24\26\30\32\34\36 \"$&(*,.\2\7\3\2\5\6\3\2\t\n\3\2\f\f\3\2\24\26\3\2"+
		" !\2\u0169\2\65\3\2\2\2\4\u0104\3\2\2\2\6\u0106\3\2\2\2\b\u0108\3\2\2"+
		"\2\n\u010a\3\2\2\2\f\u010c\3\2\2\2\16\u010e\3\2\2\2\20\u0111\3\2\2\2\22"+
		"\u0115\3\2\2\2\24\u0117\3\2\2\2\26\u011b\3\2\2\2\30\u0121\3\2\2\2\32\u0123"+
		"\3\2\2\2\34\u0127\3\2\2\2\36\u0129\3\2\2\2 \u012c\3\2\2\2\"\u012e\3\2"+
		"\2\2$\u0130\3\2\2\2&\u0133\3\2\2\2(\u0135\3\2\2\2*\u013d\3\2\2\2,\u0141"+
		"\3\2\2\2.\u0143\3\2\2\2\60\61\5\4\3\2\61\62\5\22\n\2\62\64\3\2\2\2\63"+
		"\60\3\2\2\2\64\67\3\2\2\2\65\63\3\2\2\2\65\66\3\2\2\2\66\3\3\2\2\2\67"+
		"\65\3\2\2\289\7\3\2\29:\5\24\13\2:;\7\4\2\2;=\5\26\f\2<>\t\2\2\2=<\3\2"+
		"\2\2=>\3\2\2\2>@\3\2\2\2?A\5\20\t\2@?\3\2\2\2@A\3\2\2\2AE\3\2\2\2BD\5"+
		"\16\b\2CB\3\2\2\2DG\3\2\2\2EC\3\2\2\2EF\3\2\2\2FH\3\2\2\2GE\3\2\2\2HI"+
		"\7\7\2\2IJ\5 \21\2J\u0105\3\2\2\2KR\7\b\2\2LQ\5\6\4\2MQ\5\b\5\2NQ\5\n"+
		"\6\2OQ\5\f\7\2PL\3\2\2\2PM\3\2\2\2PN\3\2\2\2PO\3\2\2\2QT\3\2\2\2RP\3\2"+
		"\2\2RS\3\2\2\2SU\3\2\2\2TR\3\2\2\2UV\5 \21\2VW\7\7\2\2WX\5\24\13\2XY\7"+
		"\4\2\2Y[\5\26\f\2Z\\\t\2\2\2[Z\3\2\2\2[\\\3\2\2\2\\^\3\2\2\2]_\5\20\t"+
		"\2^]\3\2\2\2^_\3\2\2\2_c\3\2\2\2`b\5\16\b\2a`\3\2\2\2be\3\2\2\2ca\3\2"+
		"\2\2cd\3\2\2\2dp\3\2\2\2ec\3\2\2\2fh\t\3\2\2gi\5&\24\2hg\3\2\2\2hi\3\2"+
		"\2\2im\3\2\2\2jl\5$\23\2kj\3\2\2\2lo\3\2\2\2mk\3\2\2\2mn\3\2\2\2nq\3\2"+
		"\2\2om\3\2\2\2pf\3\2\2\2pq\3\2\2\2q\u0105\3\2\2\2rv\7\13\2\2su\n\4\2\2"+
		"ts\3\2\2\2ux\3\2\2\2vt\3\2\2\2vw\3\2\2\2wy\3\2\2\2xv\3\2\2\2yz\7\7\2\2"+
		"z\u0105\5 \21\2{\177\7\r\2\2|~\n\4\2\2}|\3\2\2\2~\u0081\3\2\2\2\177}\3"+
		"\2\2\2\177\u0080\3\2\2\2\u0080\u0105\3\2\2\2\u0081\177\3\2\2\2\u0082\u0086"+
		"\7\16\2\2\u0083\u0085\n\4\2\2\u0084\u0083\3\2\2\2\u0085\u0088\3\2\2\2"+
		"\u0086\u0084\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0105\3\2\2\2\u0088\u0086"+
		"\3\2\2\2\u0089\u008d\7\17\2\2\u008a\u008c\n\4\2\2\u008b\u008a\3\2\2\2"+
		"\u008c\u008f\3\2\2\2\u008d\u008b\3\2\2\2\u008d\u008e\3\2\2\2\u008e\u0105"+
		"\3\2\2\2\u008f\u008d\3\2\2\2\u0090\u0094\7\20\2\2\u0091\u0093\n\4\2\2"+
		"\u0092\u0091\3\2\2\2\u0093\u0096\3\2\2\2\u0094\u0092\3\2\2\2\u0094\u0095"+
		"\3\2\2\2\u0095\u0105\3\2\2\2\u0096\u0094\3\2\2\2\u0097\u0098\7\21\2\2"+
		"\u0098\u0099\5\32\16\2\u0099\u009a\7\22\2\2\u009a\u009c\5\30\r\2\u009b"+
		"\u009d\t\2\2\2\u009c\u009b\3\2\2\2\u009c\u009d\3\2\2\2\u009d\u009f\3\2"+
		"\2\2\u009e\u00a0\5\20\t\2\u009f\u009e\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0"+
		"\u00a4\3\2\2\2\u00a1\u00a3\5\16\b\2\u00a2\u00a1\3\2\2\2\u00a3\u00a6\3"+
		"\2\2\2\u00a4\u00a2\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u0105\3\2\2\2\u00a6"+
		"\u00a4\3\2\2\2\u00a7\u00a8\7\23\2\2\u00a8\u00aa\5\24\13\2\u00a9\u00ab"+
		"\t\2\2\2\u00aa\u00a9\3\2\2\2\u00aa\u00ab\3\2\2\2\u00ab\u00ad\3\2\2\2\u00ac"+
		"\u00ae\5\20\t\2\u00ad\u00ac\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00b2\3"+
		"\2\2\2\u00af\u00b1\5\16\b\2\u00b0\u00af\3\2\2\2\u00b1\u00b4\3\2\2\2\u00b2"+
		"\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b7\3\2\2\2\u00b4\u00b2\3\2"+
		"\2\2\u00b5\u00b6\7\7\2\2\u00b6\u00b8\5\34\17\2\u00b7\u00b5\3\2\2\2\u00b7"+
		"\u00b8\3\2\2\2\u00b8\u0105\3\2\2\2\u00b9\u00ba\t\5\2\2\u00ba\u00bb\5 "+
		"\21\2\u00bb\u00bc\7\7\2\2\u00bc\u00bd\5\24\13\2\u00bd\u00be\7\4\2\2\u00be"+
		"\u00c0\5\26\f\2\u00bf\u00c1\t\2\2\2\u00c0\u00bf\3\2\2\2\u00c0\u00c1\3"+
		"\2\2\2\u00c1\u00c3\3\2\2\2\u00c2\u00c4\5\20\t\2\u00c3\u00c2\3\2\2\2\u00c3"+
		"\u00c4\3\2\2\2\u00c4\u00c8\3\2\2\2\u00c5\u00c7\5\16\b\2\u00c6\u00c5\3"+
		"\2\2\2\u00c7\u00ca\3\2\2\2\u00c8\u00c6\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9"+
		"\u00ce\3\2\2\2\u00ca\u00c8\3\2\2\2\u00cb\u00cd\5\36\20\2\u00cc\u00cb\3"+
		"\2\2\2\u00cd\u00d0\3\2\2\2\u00ce\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf"+
		"\u0105\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d1\u00d2\7\27\2\2\u00d2\u00d3\5"+
		"\24\13\2\u00d3\u00d4\7\4\2\2\u00d4\u00d5\5\26\f\2\u00d5\u00d6\7\7\2\2"+
		"\u00d6\u00d8\5\"\22\2\u00d7\u00d9\t\2\2\2\u00d8\u00d7\3\2\2\2\u00d8\u00d9"+
		"\3\2\2\2\u00d9\u00db\3\2\2\2\u00da\u00dc\5\20\t\2\u00db\u00da\3\2\2\2"+
		"\u00db\u00dc\3\2\2\2\u00dc\u00e0\3\2\2\2\u00dd\u00df\5\16\b\2\u00de\u00dd"+
		"\3\2\2\2\u00df\u00e2\3\2\2\2\u00e0\u00de\3\2\2\2\u00e0\u00e1\3\2\2\2\u00e1"+
		"\u0105\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e3\u00e4\7\30\2\2\u00e4\u00e5\5"+
		"\24\13\2\u00e5\u00e6\7\4\2\2\u00e6\u00e8\5\26\f\2\u00e7\u00e9\t\2\2\2"+
		"\u00e8\u00e7\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00eb\3\2\2\2\u00ea\u00ec"+
		"\5\20\t\2\u00eb\u00ea\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00f0\3\2\2\2"+
		"\u00ed\u00ef\5\16\b\2\u00ee\u00ed\3\2\2\2\u00ef\u00f2\3\2\2\2\u00f0\u00ee"+
		"\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u0105\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f3"+
		"\u00f4\7\31\2\2\u00f4\u00f5\5\24\13\2\u00f5\u00f6\7\4\2\2\u00f6\u00f8"+
		"\5\26\f\2\u00f7\u00f9\t\2\2\2\u00f8\u00f7\3\2\2\2\u00f8\u00f9\3\2\2\2"+
		"\u00f9\u00fb\3\2\2\2\u00fa\u00fc\5\20\t\2\u00fb\u00fa\3\2\2\2\u00fb\u00fc"+
		"\3\2\2\2\u00fc\u0100\3\2\2\2\u00fd\u00ff\5\16\b\2\u00fe\u00fd\3\2\2\2"+
		"\u00ff\u0102\3\2\2\2\u0100\u00fe\3\2\2\2\u0100\u0101\3\2\2\2\u0101\u0105"+
		"\3\2\2\2\u0102\u0100\3\2\2\2\u0103\u0105\7$\2\2\u01048\3\2\2\2\u0104K"+
		"\3\2\2\2\u0104r\3\2\2\2\u0104{\3\2\2\2\u0104\u0082\3\2\2\2\u0104\u0089"+
		"\3\2\2\2\u0104\u0090\3\2\2\2\u0104\u0097\3\2\2\2\u0104\u00a7\3\2\2\2\u0104"+
		"\u00b9\3\2\2\2\u0104\u00d1\3\2\2\2\u0104\u00e3\3\2\2\2\u0104\u00f3\3\2"+
		"\2\2\u0104\u0103\3\2\2\2\u0105\5\3\2\2\2\u0106\u0107\7\32\2\2\u0107\7"+
		"\3\2\2\2\u0108\u0109\7\33\2\2\u0109\t\3\2\2\2\u010a\u010b\7\34\2\2\u010b"+
		"\13\3\2\2\2\u010c\u010d\7\35\2\2\u010d\r\3\2\2\2\u010e\u010f\7\36\2\2"+
		"\u010f\u0110\5\20\t\2\u0110\17\3\2\2\2\u0111\u0112\5(\25\2\u0112\u0113"+
		"\7\22\2\2\u0113\u0114\t\6\2\2\u0114\21\3\2\2\2\u0115\u0116\7\f\2\2\u0116"+
		"\23\3\2\2\2\u0117\u0118\5*\26\2\u0118\25\3\2\2\2\u0119\u011c\5.\30\2\u011a"+
		"\u011c\5*\26\2\u011b\u0119\3\2\2\2\u011b\u011a\3\2\2\2\u011c\27\3\2\2"+
		"\2\u011d\u0122\5(\25\2\u011e\u0122\5.\30\2\u011f\u0122\7 \2\2\u0120\u0122"+
		"\7!\2\2\u0121\u011d\3\2\2\2\u0121\u011e\3\2\2\2\u0121\u011f\3\2\2\2\u0121"+
		"\u0120\3\2\2\2\u0122\31\3\2\2\2\u0123\u0124\5(\25\2\u0124\33\3\2\2\2\u0125"+
		"\u0128\5(\25\2\u0126\u0128\5*\26\2\u0127\u0125\3\2\2\2\u0127\u0126\3\2"+
		"\2\2\u0128\35\3\2\2\2\u0129\u012a\7\7\2\2\u012a\u012b\5 \21\2\u012b\37"+
		"\3\2\2\2\u012c\u012d\5*\26\2\u012d!\3\2\2\2\u012e\u012f\5*\26\2\u012f"+
		"#\3\2\2\2\u0130\u0131\7\37\2\2\u0131\u0132\5&\24\2\u0132%\3\2\2\2\u0133"+
		"\u0134\5*\26\2\u0134\'\3\2\2\2\u0135\u013a\5*\26\2\u0136\u0137\7\4\2\2"+
		"\u0137\u0139\5*\26\2\u0138\u0136\3\2\2\2\u0139\u013c\3\2\2\2\u013a\u0138"+
		"\3\2\2\2\u013a\u013b\3\2\2\2\u013b)\3\2\2\2\u013c\u013a\3\2\2\2\u013d"+
		"\u013e\5,\27\2\u013e+\3\2\2\2\u013f\u0142\7\"\2\2\u0140\u0142\5.\30\2"+
		"\u0141\u013f\3\2\2\2\u0141\u0140\3\2\2\2\u0142-\3\2\2\2\u0143\u0144\7"+
		"#\2\2\u0144/\3\2\2\2-\65=@EPR[^chmpv\177\u0086\u008d\u0094\u009c\u009f"+
		"\u00a4\u00aa\u00ad\u00b2\u00b7\u00c0\u00c3\u00c8\u00ce\u00d8\u00db\u00e0"+
		"\u00e8\u00eb\u00f0\u00f8\u00fb\u0100\u0104\u011b\u0121\u0127\u013a\u0141";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}