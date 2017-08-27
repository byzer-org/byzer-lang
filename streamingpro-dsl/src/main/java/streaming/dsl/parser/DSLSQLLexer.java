// Generated from /Users/allwefantasy/CSDNWorkSpace/streamingpro/streamingpro-dsl/src/main/resources/DSLSQL.g4 by ANTLR 4.5.3

package streaming.dsl.parser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DSLSQLLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, STRING=8, IDENTIFIER=9, 
		BACKQUOTED_IDENTIFIER=10, SIMPLE_COMMENT=11, BRACKETED_EMPTY_COMMENT=12, 
		BRACKETED_COMMENT=13, WS=14, UNRECOGNIZED=15;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "STRING", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'as'", "'save'", "'partitionBy'", "'select'", 
		"';'", null, null, null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, "STRING", "IDENTIFIER", 
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


	public DSLSQLLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "DSLSQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\21\u00a5\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\b\3\b\3\t\3\t\3\t\3\t\7\tN\n\t\f\t\16\tQ\13\t\3\t\3\t\3\t\3\t\3\t"+
		"\7\tX\n\t\f\t\16\t[\13\t\3\t\5\t^\n\t\3\n\3\n\3\n\6\nc\n\n\r\n\16\nd\3"+
		"\13\3\13\3\13\3\13\7\13k\n\13\f\13\16\13n\13\13\3\13\3\13\3\f\3\f\3\r"+
		"\3\r\3\16\3\16\3\16\3\16\7\16z\n\16\f\16\16\16}\13\16\3\16\5\16\u0080"+
		"\n\16\3\16\5\16\u0083\n\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\20\3\20\3\20\3\20\3\20\7\20\u0093\n\20\f\20\16\20\u0096\13\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\21\6\21\u009e\n\21\r\21\16\21\u009f\3\21\3\21\3"+
		"\22\3\22\3\u0094\2\23\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27"+
		"\2\31\2\33\r\35\16\37\17!\20#\21\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62;\4"+
		"\2C\\c|\4\2\f\f\17\17\3\2--\5\2\13\f\17\17\"\"\u00b1\2\3\3\2\2\2\2\5\3"+
		"\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2"+
		"\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3"+
		"\2\2\2\2!\3\2\2\2\2#\3\2\2\2\3%\3\2\2\2\5*\3\2\2\2\7,\3\2\2\2\t/\3\2\2"+
		"\2\13\64\3\2\2\2\r@\3\2\2\2\17G\3\2\2\2\21]\3\2\2\2\23b\3\2\2\2\25f\3"+
		"\2\2\2\27q\3\2\2\2\31s\3\2\2\2\33u\3\2\2\2\35\u0086\3\2\2\2\37\u008d\3"+
		"\2\2\2!\u009d\3\2\2\2#\u00a3\3\2\2\2%&\7n\2\2&\'\7q\2\2\'(\7c\2\2()\7"+
		"f\2\2)\4\3\2\2\2*+\7\60\2\2+\6\3\2\2\2,-\7c\2\2-.\7u\2\2.\b\3\2\2\2/\60"+
		"\7u\2\2\60\61\7c\2\2\61\62\7x\2\2\62\63\7g\2\2\63\n\3\2\2\2\64\65\7r\2"+
		"\2\65\66\7c\2\2\66\67\7t\2\2\678\7v\2\289\7k\2\29:\7v\2\2:;\7k\2\2;<\7"+
		"q\2\2<=\7p\2\2=>\7D\2\2>?\7{\2\2?\f\3\2\2\2@A\7u\2\2AB\7g\2\2BC\7n\2\2"+
		"CD\7g\2\2DE\7e\2\2EF\7v\2\2F\16\3\2\2\2GH\7=\2\2H\20\3\2\2\2IO\7)\2\2"+
		"JN\n\2\2\2KL\7^\2\2LN\13\2\2\2MJ\3\2\2\2MK\3\2\2\2NQ\3\2\2\2OM\3\2\2\2"+
		"OP\3\2\2\2PR\3\2\2\2QO\3\2\2\2R^\7)\2\2SY\7$\2\2TX\n\3\2\2UV\7^\2\2VX"+
		"\13\2\2\2WT\3\2\2\2WU\3\2\2\2X[\3\2\2\2YW\3\2\2\2YZ\3\2\2\2Z\\\3\2\2\2"+
		"[Y\3\2\2\2\\^\7$\2\2]I\3\2\2\2]S\3\2\2\2^\22\3\2\2\2_c\5\31\r\2`c\5\27"+
		"\f\2ac\7a\2\2b_\3\2\2\2b`\3\2\2\2ba\3\2\2\2cd\3\2\2\2db\3\2\2\2de\3\2"+
		"\2\2e\24\3\2\2\2fl\7b\2\2gk\n\4\2\2hi\7b\2\2ik\7b\2\2jg\3\2\2\2jh\3\2"+
		"\2\2kn\3\2\2\2lj\3\2\2\2lm\3\2\2\2mo\3\2\2\2nl\3\2\2\2op\7b\2\2p\26\3"+
		"\2\2\2qr\t\5\2\2r\30\3\2\2\2st\t\6\2\2t\32\3\2\2\2uv\7/\2\2vw\7/\2\2w"+
		"{\3\2\2\2xz\n\7\2\2yx\3\2\2\2z}\3\2\2\2{y\3\2\2\2{|\3\2\2\2|\177\3\2\2"+
		"\2}{\3\2\2\2~\u0080\7\17\2\2\177~\3\2\2\2\177\u0080\3\2\2\2\u0080\u0082"+
		"\3\2\2\2\u0081\u0083\7\f\2\2\u0082\u0081\3\2\2\2\u0082\u0083\3\2\2\2\u0083"+
		"\u0084\3\2\2\2\u0084\u0085\b\16\2\2\u0085\34\3\2\2\2\u0086\u0087\7\61"+
		"\2\2\u0087\u0088\7,\2\2\u0088\u0089\7,\2\2\u0089\u008a\7\61\2\2\u008a"+
		"\u008b\3\2\2\2\u008b\u008c\b\17\2\2\u008c\36\3\2\2\2\u008d\u008e\7\61"+
		"\2\2\u008e\u008f\7,\2\2\u008f\u0090\3\2\2\2\u0090\u0094\n\b\2\2\u0091"+
		"\u0093\13\2\2\2\u0092\u0091\3\2\2\2\u0093\u0096\3\2\2\2\u0094\u0095\3"+
		"\2\2\2\u0094\u0092\3\2\2\2\u0095\u0097\3\2\2\2\u0096\u0094\3\2\2\2\u0097"+
		"\u0098\7,\2\2\u0098\u0099\7\61\2\2\u0099\u009a\3\2\2\2\u009a\u009b\b\20"+
		"\2\2\u009b \3\2\2\2\u009c\u009e\t\t\2\2\u009d\u009c\3\2\2\2\u009e\u009f"+
		"\3\2\2\2\u009f\u009d\3\2\2\2\u009f\u00a0\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1"+
		"\u00a2\b\21\2\2\u00a2\"\3\2\2\2\u00a3\u00a4\13\2\2\2\u00a4$\3\2\2\2\21"+
		"\2MOWY]bdjl{\177\u0082\u0094\u009f\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}