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
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, STRING=17, 
		IDENTIFIER=18, BACKQUOTED_IDENTIFIER=19, SIMPLE_COMMENT=20, BRACKETED_EMPTY_COMMENT=21, 
		BRACKETED_COMMENT=22, WS=23, UNRECOGNIZED=24;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "STRING", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", 
		"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'options'", "'as'", "'save'", "'partitionBy'", 
		"'select'", "';'", "'connect'", "'where'", "'overwrite'", "'append'", 
		"'errorIfExists'", "'ignore'", "'and'", "'='", null, null, null, null, 
		"'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\32\u00f9\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3"+
		"\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\7"+
		"\22\u00a2\n\22\f\22\16\22\u00a5\13\22\3\22\3\22\3\22\3\22\3\22\7\22\u00ac"+
		"\n\22\f\22\16\22\u00af\13\22\3\22\5\22\u00b2\n\22\3\23\3\23\3\23\6\23"+
		"\u00b7\n\23\r\23\16\23\u00b8\3\24\3\24\3\24\3\24\7\24\u00bf\n\24\f\24"+
		"\16\24\u00c2\13\24\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\27\3\27\7"+
		"\27\u00ce\n\27\f\27\16\27\u00d1\13\27\3\27\5\27\u00d4\n\27\3\27\5\27\u00d7"+
		"\n\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31"+
		"\3\31\7\31\u00e7\n\31\f\31\16\31\u00ea\13\31\3\31\3\31\3\31\3\31\3\31"+
		"\3\32\6\32\u00f2\n\32\r\32\16\32\u00f3\3\32\3\32\3\33\3\33\3\u00e8\2\34"+
		"\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20"+
		"\37\21!\22#\23%\24\'\25)\2+\2-\26/\27\61\30\63\31\65\32\3\2\n\4\2))^^"+
		"\4\2$$^^\3\2bb\3\2\62;\4\2C\\c|\4\2\f\f\17\17\3\2--\5\2\13\f\17\17\"\""+
		"\u0105\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2"+
		"\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3"+
		"\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2"+
		"\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2"+
		"\2\63\3\2\2\2\2\65\3\2\2\2\3\67\3\2\2\2\5<\3\2\2\2\7>\3\2\2\2\tF\3\2\2"+
		"\2\13I\3\2\2\2\rN\3\2\2\2\17Z\3\2\2\2\21a\3\2\2\2\23c\3\2\2\2\25k\3\2"+
		"\2\2\27q\3\2\2\2\31{\3\2\2\2\33\u0082\3\2\2\2\35\u0090\3\2\2\2\37\u0097"+
		"\3\2\2\2!\u009b\3\2\2\2#\u00b1\3\2\2\2%\u00b6\3\2\2\2\'\u00ba\3\2\2\2"+
		")\u00c5\3\2\2\2+\u00c7\3\2\2\2-\u00c9\3\2\2\2/\u00da\3\2\2\2\61\u00e1"+
		"\3\2\2\2\63\u00f1\3\2\2\2\65\u00f7\3\2\2\2\678\7n\2\289\7q\2\29:\7c\2"+
		"\2:;\7f\2\2;\4\3\2\2\2<=\7\60\2\2=\6\3\2\2\2>?\7q\2\2?@\7r\2\2@A\7v\2"+
		"\2AB\7k\2\2BC\7q\2\2CD\7p\2\2DE\7u\2\2E\b\3\2\2\2FG\7c\2\2GH\7u\2\2H\n"+
		"\3\2\2\2IJ\7u\2\2JK\7c\2\2KL\7x\2\2LM\7g\2\2M\f\3\2\2\2NO\7r\2\2OP\7c"+
		"\2\2PQ\7t\2\2QR\7v\2\2RS\7k\2\2ST\7v\2\2TU\7k\2\2UV\7q\2\2VW\7p\2\2WX"+
		"\7D\2\2XY\7{\2\2Y\16\3\2\2\2Z[\7u\2\2[\\\7g\2\2\\]\7n\2\2]^\7g\2\2^_\7"+
		"e\2\2_`\7v\2\2`\20\3\2\2\2ab\7=\2\2b\22\3\2\2\2cd\7e\2\2de\7q\2\2ef\7"+
		"p\2\2fg\7p\2\2gh\7g\2\2hi\7e\2\2ij\7v\2\2j\24\3\2\2\2kl\7y\2\2lm\7j\2"+
		"\2mn\7g\2\2no\7t\2\2op\7g\2\2p\26\3\2\2\2qr\7q\2\2rs\7x\2\2st\7g\2\2t"+
		"u\7t\2\2uv\7y\2\2vw\7t\2\2wx\7k\2\2xy\7v\2\2yz\7g\2\2z\30\3\2\2\2{|\7"+
		"c\2\2|}\7r\2\2}~\7r\2\2~\177\7g\2\2\177\u0080\7p\2\2\u0080\u0081\7f\2"+
		"\2\u0081\32\3\2\2\2\u0082\u0083\7g\2\2\u0083\u0084\7t\2\2\u0084\u0085"+
		"\7t\2\2\u0085\u0086\7q\2\2\u0086\u0087\7t\2\2\u0087\u0088\7K\2\2\u0088"+
		"\u0089\7h\2\2\u0089\u008a\7G\2\2\u008a\u008b\7z\2\2\u008b\u008c\7k\2\2"+
		"\u008c\u008d\7u\2\2\u008d\u008e\7v\2\2\u008e\u008f\7u\2\2\u008f\34\3\2"+
		"\2\2\u0090\u0091\7k\2\2\u0091\u0092\7i\2\2\u0092\u0093\7p\2\2\u0093\u0094"+
		"\7q\2\2\u0094\u0095\7t\2\2\u0095\u0096\7g\2\2\u0096\36\3\2\2\2\u0097\u0098"+
		"\7c\2\2\u0098\u0099\7p\2\2\u0099\u009a\7f\2\2\u009a \3\2\2\2\u009b\u009c"+
		"\7?\2\2\u009c\"\3\2\2\2\u009d\u00a3\7)\2\2\u009e\u00a2\n\2\2\2\u009f\u00a0"+
		"\7^\2\2\u00a0\u00a2\13\2\2\2\u00a1\u009e\3\2\2\2\u00a1\u009f\3\2\2\2\u00a2"+
		"\u00a5\3\2\2\2\u00a3\u00a1\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4\u00a6\3\2"+
		"\2\2\u00a5\u00a3\3\2\2\2\u00a6\u00b2\7)\2\2\u00a7\u00ad\7$\2\2\u00a8\u00ac"+
		"\n\3\2\2\u00a9\u00aa\7^\2\2\u00aa\u00ac\13\2\2\2\u00ab\u00a8\3\2\2\2\u00ab"+
		"\u00a9\3\2\2\2\u00ac\u00af\3\2\2\2\u00ad\u00ab\3\2\2\2\u00ad\u00ae\3\2"+
		"\2\2\u00ae\u00b0\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\u00b2\7$\2\2\u00b1"+
		"\u009d\3\2\2\2\u00b1\u00a7\3\2\2\2\u00b2$\3\2\2\2\u00b3\u00b7\5+\26\2"+
		"\u00b4\u00b7\5)\25\2\u00b5\u00b7\7a\2\2\u00b6\u00b3\3\2\2\2\u00b6\u00b4"+
		"\3\2\2\2\u00b6\u00b5\3\2\2\2\u00b7\u00b8\3\2\2\2\u00b8\u00b6\3\2\2\2\u00b8"+
		"\u00b9\3\2\2\2\u00b9&\3\2\2\2\u00ba\u00c0\7b\2\2\u00bb\u00bf\n\4\2\2\u00bc"+
		"\u00bd\7b\2\2\u00bd\u00bf\7b\2\2\u00be\u00bb\3\2\2\2\u00be\u00bc\3\2\2"+
		"\2\u00bf\u00c2\3\2\2\2\u00c0\u00be\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1\u00c3"+
		"\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c3\u00c4\7b\2\2\u00c4(\3\2\2\2\u00c5\u00c6"+
		"\t\5\2\2\u00c6*\3\2\2\2\u00c7\u00c8\t\6\2\2\u00c8,\3\2\2\2\u00c9\u00ca"+
		"\7/\2\2\u00ca\u00cb\7/\2\2\u00cb\u00cf\3\2\2\2\u00cc\u00ce\n\7\2\2\u00cd"+
		"\u00cc\3\2\2\2\u00ce\u00d1\3\2\2\2\u00cf\u00cd\3\2\2\2\u00cf\u00d0\3\2"+
		"\2\2\u00d0\u00d3\3\2\2\2\u00d1\u00cf\3\2\2\2\u00d2\u00d4\7\17\2\2\u00d3"+
		"\u00d2\3\2\2\2\u00d3\u00d4\3\2\2\2\u00d4\u00d6\3\2\2\2\u00d5\u00d7\7\f"+
		"\2\2\u00d6\u00d5\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\u00d8\3\2\2\2\u00d8"+
		"\u00d9\b\27\2\2\u00d9.\3\2\2\2\u00da\u00db\7\61\2\2\u00db\u00dc\7,\2\2"+
		"\u00dc\u00dd\7,\2\2\u00dd\u00de\7\61\2\2\u00de\u00df\3\2\2\2\u00df\u00e0"+
		"\b\30\2\2\u00e0\60\3\2\2\2\u00e1\u00e2\7\61\2\2\u00e2\u00e3\7,\2\2\u00e3"+
		"\u00e4\3\2\2\2\u00e4\u00e8\n\b\2\2\u00e5\u00e7\13\2\2\2\u00e6\u00e5\3"+
		"\2\2\2\u00e7\u00ea\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e8\u00e6\3\2\2\2\u00e9"+
		"\u00eb\3\2\2\2\u00ea\u00e8\3\2\2\2\u00eb\u00ec\7,\2\2\u00ec\u00ed\7\61"+
		"\2\2\u00ed\u00ee\3\2\2\2\u00ee\u00ef\b\31\2\2\u00ef\62\3\2\2\2\u00f0\u00f2"+
		"\t\t\2\2\u00f1\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f3"+
		"\u00f4\3\2\2\2\u00f4\u00f5\3\2\2\2\u00f5\u00f6\b\32\2\2\u00f6\64\3\2\2"+
		"\2\u00f7\u00f8\13\2\2\2\u00f8\66\3\2\2\2\21\2\u00a1\u00a3\u00ab\u00ad"+
		"\u00b1\u00b6\u00b8\u00be\u00c0\u00cf\u00d3\u00d6\u00e8\u00f3\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}