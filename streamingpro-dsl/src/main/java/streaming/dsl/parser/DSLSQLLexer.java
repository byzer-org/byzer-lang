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
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, STRING=16, 
		IDENTIFIER=17, BACKQUOTED_IDENTIFIER=18, SIMPLE_COMMENT=19, BRACKETED_EMPTY_COMMENT=20, 
		BRACKETED_COMMENT=21, WS=22, UNRECOGNIZED=23;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "STRING", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'as'", "'save'", "'partitionBy'", "'select'", 
		"';'", "'connect'", "'where'", "'overwrite'", "'append'", "'errorIfExists'", 
		"'ignore'", "'and'", "'='", null, null, null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\31\u00ef\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3"+
		"\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3"+
		"\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\20\3\20\3\21"+
		"\3\21\3\21\3\21\7\21\u0098\n\21\f\21\16\21\u009b\13\21\3\21\3\21\3\21"+
		"\3\21\3\21\7\21\u00a2\n\21\f\21\16\21\u00a5\13\21\3\21\5\21\u00a8\n\21"+
		"\3\22\3\22\3\22\6\22\u00ad\n\22\r\22\16\22\u00ae\3\23\3\23\3\23\3\23\7"+
		"\23\u00b5\n\23\f\23\16\23\u00b8\13\23\3\23\3\23\3\24\3\24\3\25\3\25\3"+
		"\26\3\26\3\26\3\26\7\26\u00c4\n\26\f\26\16\26\u00c7\13\26\3\26\5\26\u00ca"+
		"\n\26\3\26\5\26\u00cd\n\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27"+
		"\3\30\3\30\3\30\3\30\3\30\7\30\u00dd\n\30\f\30\16\30\u00e0\13\30\3\30"+
		"\3\30\3\30\3\30\3\30\3\31\6\31\u00e8\n\31\r\31\16\31\u00e9\3\31\3\31\3"+
		"\32\3\32\3\u00de\2\33\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27"+
		"\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\2)\2+\25-\26/\27\61\30\63\31"+
		"\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62;\4\2C\\c|\4\2\f\f\17\17\3\2--\5\2"+
		"\13\f\17\17\"\"\u00fb\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2"+
		"\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25"+
		"\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2"+
		"\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2"+
		"\2\61\3\2\2\2\2\63\3\2\2\2\3\65\3\2\2\2\5:\3\2\2\2\7<\3\2\2\2\t?\3\2\2"+
		"\2\13D\3\2\2\2\rP\3\2\2\2\17W\3\2\2\2\21Y\3\2\2\2\23a\3\2\2\2\25g\3\2"+
		"\2\2\27q\3\2\2\2\31x\3\2\2\2\33\u0086\3\2\2\2\35\u008d\3\2\2\2\37\u0091"+
		"\3\2\2\2!\u00a7\3\2\2\2#\u00ac\3\2\2\2%\u00b0\3\2\2\2\'\u00bb\3\2\2\2"+
		")\u00bd\3\2\2\2+\u00bf\3\2\2\2-\u00d0\3\2\2\2/\u00d7\3\2\2\2\61\u00e7"+
		"\3\2\2\2\63\u00ed\3\2\2\2\65\66\7n\2\2\66\67\7q\2\2\678\7c\2\289\7f\2"+
		"\29\4\3\2\2\2:;\7\60\2\2;\6\3\2\2\2<=\7c\2\2=>\7u\2\2>\b\3\2\2\2?@\7u"+
		"\2\2@A\7c\2\2AB\7x\2\2BC\7g\2\2C\n\3\2\2\2DE\7r\2\2EF\7c\2\2FG\7t\2\2"+
		"GH\7v\2\2HI\7k\2\2IJ\7v\2\2JK\7k\2\2KL\7q\2\2LM\7p\2\2MN\7D\2\2NO\7{\2"+
		"\2O\f\3\2\2\2PQ\7u\2\2QR\7g\2\2RS\7n\2\2ST\7g\2\2TU\7e\2\2UV\7v\2\2V\16"+
		"\3\2\2\2WX\7=\2\2X\20\3\2\2\2YZ\7e\2\2Z[\7q\2\2[\\\7p\2\2\\]\7p\2\2]^"+
		"\7g\2\2^_\7e\2\2_`\7v\2\2`\22\3\2\2\2ab\7y\2\2bc\7j\2\2cd\7g\2\2de\7t"+
		"\2\2ef\7g\2\2f\24\3\2\2\2gh\7q\2\2hi\7x\2\2ij\7g\2\2jk\7t\2\2kl\7y\2\2"+
		"lm\7t\2\2mn\7k\2\2no\7v\2\2op\7g\2\2p\26\3\2\2\2qr\7c\2\2rs\7r\2\2st\7"+
		"r\2\2tu\7g\2\2uv\7p\2\2vw\7f\2\2w\30\3\2\2\2xy\7g\2\2yz\7t\2\2z{\7t\2"+
		"\2{|\7q\2\2|}\7t\2\2}~\7K\2\2~\177\7h\2\2\177\u0080\7G\2\2\u0080\u0081"+
		"\7z\2\2\u0081\u0082\7k\2\2\u0082\u0083\7u\2\2\u0083\u0084\7v\2\2\u0084"+
		"\u0085\7u\2\2\u0085\32\3\2\2\2\u0086\u0087\7k\2\2\u0087\u0088\7i\2\2\u0088"+
		"\u0089\7p\2\2\u0089\u008a\7q\2\2\u008a\u008b\7t\2\2\u008b\u008c\7g\2\2"+
		"\u008c\34\3\2\2\2\u008d\u008e\7c\2\2\u008e\u008f\7p\2\2\u008f\u0090\7"+
		"f\2\2\u0090\36\3\2\2\2\u0091\u0092\7?\2\2\u0092 \3\2\2\2\u0093\u0099\7"+
		")\2\2\u0094\u0098\n\2\2\2\u0095\u0096\7^\2\2\u0096\u0098\13\2\2\2\u0097"+
		"\u0094\3\2\2\2\u0097\u0095\3\2\2\2\u0098\u009b\3\2\2\2\u0099\u0097\3\2"+
		"\2\2\u0099\u009a\3\2\2\2\u009a\u009c\3\2\2\2\u009b\u0099\3\2\2\2\u009c"+
		"\u00a8\7)\2\2\u009d\u00a3\7$\2\2\u009e\u00a2\n\3\2\2\u009f\u00a0\7^\2"+
		"\2\u00a0\u00a2\13\2\2\2\u00a1\u009e\3\2\2\2\u00a1\u009f\3\2\2\2\u00a2"+
		"\u00a5\3\2\2\2\u00a3\u00a1\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a6\3\2"+
		"\2\2\u00a5\u00a3\3\2\2\2\u00a6\u00a8\7$\2\2\u00a7\u0093\3\2\2\2\u00a7"+
		"\u009d\3\2\2\2\u00a8\"\3\2\2\2\u00a9\u00ad\5)\25\2\u00aa\u00ad\5\'\24"+
		"\2\u00ab\u00ad\7a\2\2\u00ac\u00a9\3\2\2\2\u00ac\u00aa\3\2\2\2\u00ac\u00ab"+
		"\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00ac\3\2\2\2\u00ae\u00af\3\2\2\2\u00af"+
		"$\3\2\2\2\u00b0\u00b6\7b\2\2\u00b1\u00b5\n\4\2\2\u00b2\u00b3\7b\2\2\u00b3"+
		"\u00b5\7b\2\2\u00b4\u00b1\3\2\2\2\u00b4\u00b2\3\2\2\2\u00b5\u00b8\3\2"+
		"\2\2\u00b6\u00b4\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b9\3\2\2\2\u00b8"+
		"\u00b6\3\2\2\2\u00b9\u00ba\7b\2\2\u00ba&\3\2\2\2\u00bb\u00bc\t\5\2\2\u00bc"+
		"(\3\2\2\2\u00bd\u00be\t\6\2\2\u00be*\3\2\2\2\u00bf\u00c0\7/\2\2\u00c0"+
		"\u00c1\7/\2\2\u00c1\u00c5\3\2\2\2\u00c2\u00c4\n\7\2\2\u00c3\u00c2\3\2"+
		"\2\2\u00c4\u00c7\3\2\2\2\u00c5\u00c3\3\2\2\2\u00c5\u00c6\3\2\2\2\u00c6"+
		"\u00c9\3\2\2\2\u00c7\u00c5\3\2\2\2\u00c8\u00ca\7\17\2\2\u00c9\u00c8\3"+
		"\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cc\3\2\2\2\u00cb\u00cd\7\f\2\2\u00cc"+
		"\u00cb\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd\u00ce\3\2\2\2\u00ce\u00cf\b\26"+
		"\2\2\u00cf,\3\2\2\2\u00d0\u00d1\7\61\2\2\u00d1\u00d2\7,\2\2\u00d2\u00d3"+
		"\7,\2\2\u00d3\u00d4\7\61\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d6\b\27\2\2"+
		"\u00d6.\3\2\2\2\u00d7\u00d8\7\61\2\2\u00d8\u00d9\7,\2\2\u00d9\u00da\3"+
		"\2\2\2\u00da\u00de\n\b\2\2\u00db\u00dd\13\2\2\2\u00dc\u00db\3\2\2\2\u00dd"+
		"\u00e0\3\2\2\2\u00de\u00df\3\2\2\2\u00de\u00dc\3\2\2\2\u00df\u00e1\3\2"+
		"\2\2\u00e0\u00de\3\2\2\2\u00e1\u00e2\7,\2\2\u00e2\u00e3\7\61\2\2\u00e3"+
		"\u00e4\3\2\2\2\u00e4\u00e5\b\30\2\2\u00e5\60\3\2\2\2\u00e6\u00e8\t\t\2"+
		"\2\u00e7\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00e7\3\2\2\2\u00e9\u00ea"+
		"\3\2\2\2\u00ea\u00eb\3\2\2\2\u00eb\u00ec\b\31\2\2\u00ec\62\3\2\2\2\u00ed"+
		"\u00ee\13\2\2\2\u00ee\64\3\2\2\2\21\2\u0097\u0099\u00a1\u00a3\u00a7\u00ac"+
		"\u00ae\u00b4\u00b6\u00c5\u00c9\u00cc\u00de\u00e9\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}