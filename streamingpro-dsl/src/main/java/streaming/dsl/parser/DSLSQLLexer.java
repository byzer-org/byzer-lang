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
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, STRING=12, IDENTIFIER=13, BACKQUOTED_IDENTIFIER=14, 
		SIMPLE_COMMENT=15, BRACKETED_EMPTY_COMMENT=16, BRACKETED_COMMENT=17, WS=18, 
		UNRECOGNIZED=19;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", 
		"LETTER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", 
		"WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'as'", "'save'", "'partitionBy'", "'select'", 
		"';'", "'connect'", "'where'", "'and'", "'='", null, null, null, null, 
		"'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\25\u00c1\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\3\2\3\2\3\2\3\2\3\3"+
		"\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\f\3\f\3\r\3"+
		"\r\3\r\3\r\7\rj\n\r\f\r\16\rm\13\r\3\r\3\r\3\r\3\r\3\r\7\rt\n\r\f\r\16"+
		"\rw\13\r\3\r\5\rz\n\r\3\16\3\16\3\16\6\16\177\n\16\r\16\16\16\u0080\3"+
		"\17\3\17\3\17\3\17\7\17\u0087\n\17\f\17\16\17\u008a\13\17\3\17\3\17\3"+
		"\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\7\22\u0096\n\22\f\22\16\22\u0099"+
		"\13\22\3\22\5\22\u009c\n\22\3\22\5\22\u009f\n\22\3\22\3\22\3\23\3\23\3"+
		"\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\7\24\u00af\n\24\f\24"+
		"\16\24\u00b2\13\24\3\24\3\24\3\24\3\24\3\24\3\25\6\25\u00ba\n\25\r\25"+
		"\16\25\u00bb\3\25\3\25\3\26\3\26\3\u00b0\2\27\3\3\5\4\7\5\t\6\13\7\r\b"+
		"\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\2!\2#\21%\22\'\23)\24"+
		"+\25\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62;\4\2C\\c|\4\2\f\f\17\17\3\2--"+
		"\5\2\13\f\17\17\"\"\u00cd\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2"+
		"\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2"+
		"\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2#\3\2"+
		"\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\3-\3\2\2\2\5\62\3\2"+
		"\2\2\7\64\3\2\2\2\t\67\3\2\2\2\13<\3\2\2\2\rH\3\2\2\2\17O\3\2\2\2\21Q"+
		"\3\2\2\2\23Y\3\2\2\2\25_\3\2\2\2\27c\3\2\2\2\31y\3\2\2\2\33~\3\2\2\2\35"+
		"\u0082\3\2\2\2\37\u008d\3\2\2\2!\u008f\3\2\2\2#\u0091\3\2\2\2%\u00a2\3"+
		"\2\2\2\'\u00a9\3\2\2\2)\u00b9\3\2\2\2+\u00bf\3\2\2\2-.\7n\2\2./\7q\2\2"+
		"/\60\7c\2\2\60\61\7f\2\2\61\4\3\2\2\2\62\63\7\60\2\2\63\6\3\2\2\2\64\65"+
		"\7c\2\2\65\66\7u\2\2\66\b\3\2\2\2\678\7u\2\289\7c\2\29:\7x\2\2:;\7g\2"+
		"\2;\n\3\2\2\2<=\7r\2\2=>\7c\2\2>?\7t\2\2?@\7v\2\2@A\7k\2\2AB\7v\2\2BC"+
		"\7k\2\2CD\7q\2\2DE\7p\2\2EF\7D\2\2FG\7{\2\2G\f\3\2\2\2HI\7u\2\2IJ\7g\2"+
		"\2JK\7n\2\2KL\7g\2\2LM\7e\2\2MN\7v\2\2N\16\3\2\2\2OP\7=\2\2P\20\3\2\2"+
		"\2QR\7e\2\2RS\7q\2\2ST\7p\2\2TU\7p\2\2UV\7g\2\2VW\7e\2\2WX\7v\2\2X\22"+
		"\3\2\2\2YZ\7y\2\2Z[\7j\2\2[\\\7g\2\2\\]\7t\2\2]^\7g\2\2^\24\3\2\2\2_`"+
		"\7c\2\2`a\7p\2\2ab\7f\2\2b\26\3\2\2\2cd\7?\2\2d\30\3\2\2\2ek\7)\2\2fj"+
		"\n\2\2\2gh\7^\2\2hj\13\2\2\2if\3\2\2\2ig\3\2\2\2jm\3\2\2\2ki\3\2\2\2k"+
		"l\3\2\2\2ln\3\2\2\2mk\3\2\2\2nz\7)\2\2ou\7$\2\2pt\n\3\2\2qr\7^\2\2rt\13"+
		"\2\2\2sp\3\2\2\2sq\3\2\2\2tw\3\2\2\2us\3\2\2\2uv\3\2\2\2vx\3\2\2\2wu\3"+
		"\2\2\2xz\7$\2\2ye\3\2\2\2yo\3\2\2\2z\32\3\2\2\2{\177\5!\21\2|\177\5\37"+
		"\20\2}\177\7a\2\2~{\3\2\2\2~|\3\2\2\2~}\3\2\2\2\177\u0080\3\2\2\2\u0080"+
		"~\3\2\2\2\u0080\u0081\3\2\2\2\u0081\34\3\2\2\2\u0082\u0088\7b\2\2\u0083"+
		"\u0087\n\4\2\2\u0084\u0085\7b\2\2\u0085\u0087\7b\2\2\u0086\u0083\3\2\2"+
		"\2\u0086\u0084\3\2\2\2\u0087\u008a\3\2\2\2\u0088\u0086\3\2\2\2\u0088\u0089"+
		"\3\2\2\2\u0089\u008b\3\2\2\2\u008a\u0088\3\2\2\2\u008b\u008c\7b\2\2\u008c"+
		"\36\3\2\2\2\u008d\u008e\t\5\2\2\u008e \3\2\2\2\u008f\u0090\t\6\2\2\u0090"+
		"\"\3\2\2\2\u0091\u0092\7/\2\2\u0092\u0093\7/\2\2\u0093\u0097\3\2\2\2\u0094"+
		"\u0096\n\7\2\2\u0095\u0094\3\2\2\2\u0096\u0099\3\2\2\2\u0097\u0095\3\2"+
		"\2\2\u0097\u0098\3\2\2\2\u0098\u009b\3\2\2\2\u0099\u0097\3\2\2\2\u009a"+
		"\u009c\7\17\2\2\u009b\u009a\3\2\2\2\u009b\u009c\3\2\2\2\u009c\u009e\3"+
		"\2\2\2\u009d\u009f\7\f\2\2\u009e\u009d\3\2\2\2\u009e\u009f\3\2\2\2\u009f"+
		"\u00a0\3\2\2\2\u00a0\u00a1\b\22\2\2\u00a1$\3\2\2\2\u00a2\u00a3\7\61\2"+
		"\2\u00a3\u00a4\7,\2\2\u00a4\u00a5\7,\2\2\u00a5\u00a6\7\61\2\2\u00a6\u00a7"+
		"\3\2\2\2\u00a7\u00a8\b\23\2\2\u00a8&\3\2\2\2\u00a9\u00aa\7\61\2\2\u00aa"+
		"\u00ab\7,\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00b0\n\b\2\2\u00ad\u00af\13\2"+
		"\2\2\u00ae\u00ad\3\2\2\2\u00af\u00b2\3\2\2\2\u00b0\u00b1\3\2\2\2\u00b0"+
		"\u00ae\3\2\2\2\u00b1\u00b3\3\2\2\2\u00b2\u00b0\3\2\2\2\u00b3\u00b4\7,"+
		"\2\2\u00b4\u00b5\7\61\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b7\b\24\2\2\u00b7"+
		"(\3\2\2\2\u00b8\u00ba\t\t\2\2\u00b9\u00b8\3\2\2\2\u00ba\u00bb\3\2\2\2"+
		"\u00bb\u00b9\3\2\2\2\u00bb\u00bc\3\2\2\2\u00bc\u00bd\3\2\2\2\u00bd\u00be"+
		"\b\25\2\2\u00be*\3\2\2\2\u00bf\u00c0\13\2\2\2\u00c0,\3\2\2\2\21\2iksu"+
		"y~\u0080\u0086\u0088\u0097\u009b\u009e\u00b0\u00bb\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}