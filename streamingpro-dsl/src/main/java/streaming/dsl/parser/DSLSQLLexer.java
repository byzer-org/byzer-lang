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
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		STRING=25, IDENTIFIER=26, BACKQUOTED_IDENTIFIER=27, SIMPLE_COMMENT=28, 
		BRACKETED_EMPTY_COMMENT=29, BRACKETED_COMMENT=30, WS=31, UNRECOGNIZED=32;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "STRING", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", 
		"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'LOAD'", "'.'", "'options'", "'as'", "'save'", "'SAVE'", 
		"'partitionBy'", "'select'", "'SELECT'", "';'", "'insert'", "'INSERT'", 
		"'create'", "'CREATE'", "'connect'", "'CONNECT'", "'where'", "'overwrite'", 
		"'append'", "'errorIfExists'", "'ignore'", "'and'", "'='", null, null, 
		null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
		"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\"\u013e\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3"+
		"\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\r\3\r\3"+
		"\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3"+
		"\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\23\3"+
		"\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3"+
		"\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3"+
		"\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3"+
		"\27\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\32\3\32\7\32\u00e7\n\32"+
		"\f\32\16\32\u00ea\13\32\3\32\3\32\3\32\3\32\3\32\7\32\u00f1\n\32\f\32"+
		"\16\32\u00f4\13\32\3\32\5\32\u00f7\n\32\3\33\3\33\3\33\6\33\u00fc\n\33"+
		"\r\33\16\33\u00fd\3\34\3\34\3\34\3\34\7\34\u0104\n\34\f\34\16\34\u0107"+
		"\13\34\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3\37\3\37\7\37\u0113\n"+
		"\37\f\37\16\37\u0116\13\37\3\37\5\37\u0119\n\37\3\37\5\37\u011c\n\37\3"+
		"\37\3\37\3 \3 \3 \3 \3 \3 \3 \3!\3!\3!\3!\3!\7!\u012c\n!\f!\16!\u012f"+
		"\13!\3!\3!\3!\3!\3!\3\"\6\"\u0137\n\"\r\"\16\"\u0138\3\"\3\"\3#\3#\3\u012d"+
		"\2$\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35"+
		"\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\2"+
		";\2=\36?\37A C!E\"\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62;\4\2C\\c|\4\2\f"+
		"\f\17\17\3\2--\5\2\13\f\17\17\"\"\u014a\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3"+
		"\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2"+
		"\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35"+
		"\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)"+
		"\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2"+
		"\65\3\2\2\2\2\67\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2"+
		"E\3\2\2\2\3G\3\2\2\2\5L\3\2\2\2\7Q\3\2\2\2\tS\3\2\2\2\13[\3\2\2\2\r^\3"+
		"\2\2\2\17c\3\2\2\2\21h\3\2\2\2\23t\3\2\2\2\25{\3\2\2\2\27\u0082\3\2\2"+
		"\2\31\u0084\3\2\2\2\33\u008b\3\2\2\2\35\u0092\3\2\2\2\37\u0099\3\2\2\2"+
		"!\u00a0\3\2\2\2#\u00a8\3\2\2\2%\u00b0\3\2\2\2\'\u00b6\3\2\2\2)\u00c0\3"+
		"\2\2\2+\u00c7\3\2\2\2-\u00d5\3\2\2\2/\u00dc\3\2\2\2\61\u00e0\3\2\2\2\63"+
		"\u00f6\3\2\2\2\65\u00fb\3\2\2\2\67\u00ff\3\2\2\29\u010a\3\2\2\2;\u010c"+
		"\3\2\2\2=\u010e\3\2\2\2?\u011f\3\2\2\2A\u0126\3\2\2\2C\u0136\3\2\2\2E"+
		"\u013c\3\2\2\2GH\7n\2\2HI\7q\2\2IJ\7c\2\2JK\7f\2\2K\4\3\2\2\2LM\7N\2\2"+
		"MN\7Q\2\2NO\7C\2\2OP\7F\2\2P\6\3\2\2\2QR\7\60\2\2R\b\3\2\2\2ST\7q\2\2"+
		"TU\7r\2\2UV\7v\2\2VW\7k\2\2WX\7q\2\2XY\7p\2\2YZ\7u\2\2Z\n\3\2\2\2[\\\7"+
		"c\2\2\\]\7u\2\2]\f\3\2\2\2^_\7u\2\2_`\7c\2\2`a\7x\2\2ab\7g\2\2b\16\3\2"+
		"\2\2cd\7U\2\2de\7C\2\2ef\7X\2\2fg\7G\2\2g\20\3\2\2\2hi\7r\2\2ij\7c\2\2"+
		"jk\7t\2\2kl\7v\2\2lm\7k\2\2mn\7v\2\2no\7k\2\2op\7q\2\2pq\7p\2\2qr\7D\2"+
		"\2rs\7{\2\2s\22\3\2\2\2tu\7u\2\2uv\7g\2\2vw\7n\2\2wx\7g\2\2xy\7e\2\2y"+
		"z\7v\2\2z\24\3\2\2\2{|\7U\2\2|}\7G\2\2}~\7N\2\2~\177\7G\2\2\177\u0080"+
		"\7E\2\2\u0080\u0081\7V\2\2\u0081\26\3\2\2\2\u0082\u0083\7=\2\2\u0083\30"+
		"\3\2\2\2\u0084\u0085\7k\2\2\u0085\u0086\7p\2\2\u0086\u0087\7u\2\2\u0087"+
		"\u0088\7g\2\2\u0088\u0089\7t\2\2\u0089\u008a\7v\2\2\u008a\32\3\2\2\2\u008b"+
		"\u008c\7K\2\2\u008c\u008d\7P\2\2\u008d\u008e\7U\2\2\u008e\u008f\7G\2\2"+
		"\u008f\u0090\7T\2\2\u0090\u0091\7V\2\2\u0091\34\3\2\2\2\u0092\u0093\7"+
		"e\2\2\u0093\u0094\7t\2\2\u0094\u0095\7g\2\2\u0095\u0096\7c\2\2\u0096\u0097"+
		"\7v\2\2\u0097\u0098\7g\2\2\u0098\36\3\2\2\2\u0099\u009a\7E\2\2\u009a\u009b"+
		"\7T\2\2\u009b\u009c\7G\2\2\u009c\u009d\7C\2\2\u009d\u009e\7V\2\2\u009e"+
		"\u009f\7G\2\2\u009f \3\2\2\2\u00a0\u00a1\7e\2\2\u00a1\u00a2\7q\2\2\u00a2"+
		"\u00a3\7p\2\2\u00a3\u00a4\7p\2\2\u00a4\u00a5\7g\2\2\u00a5\u00a6\7e\2\2"+
		"\u00a6\u00a7\7v\2\2\u00a7\"\3\2\2\2\u00a8\u00a9\7E\2\2\u00a9\u00aa\7Q"+
		"\2\2\u00aa\u00ab\7P\2\2\u00ab\u00ac\7P\2\2\u00ac\u00ad\7G\2\2\u00ad\u00ae"+
		"\7E\2\2\u00ae\u00af\7V\2\2\u00af$\3\2\2\2\u00b0\u00b1\7y\2\2\u00b1\u00b2"+
		"\7j\2\2\u00b2\u00b3\7g\2\2\u00b3\u00b4\7t\2\2\u00b4\u00b5\7g\2\2\u00b5"+
		"&\3\2\2\2\u00b6\u00b7\7q\2\2\u00b7\u00b8\7x\2\2\u00b8\u00b9\7g\2\2\u00b9"+
		"\u00ba\7t\2\2\u00ba\u00bb\7y\2\2\u00bb\u00bc\7t\2\2\u00bc\u00bd\7k\2\2"+
		"\u00bd\u00be\7v\2\2\u00be\u00bf\7g\2\2\u00bf(\3\2\2\2\u00c0\u00c1\7c\2"+
		"\2\u00c1\u00c2\7r\2\2\u00c2\u00c3\7r\2\2\u00c3\u00c4\7g\2\2\u00c4\u00c5"+
		"\7p\2\2\u00c5\u00c6\7f\2\2\u00c6*\3\2\2\2\u00c7\u00c8\7g\2\2\u00c8\u00c9"+
		"\7t\2\2\u00c9\u00ca\7t\2\2\u00ca\u00cb\7q\2\2\u00cb\u00cc\7t\2\2\u00cc"+
		"\u00cd\7K\2\2\u00cd\u00ce\7h\2\2\u00ce\u00cf\7G\2\2\u00cf\u00d0\7z\2\2"+
		"\u00d0\u00d1\7k\2\2\u00d1\u00d2\7u\2\2\u00d2\u00d3\7v\2\2\u00d3\u00d4"+
		"\7u\2\2\u00d4,\3\2\2\2\u00d5\u00d6\7k\2\2\u00d6\u00d7\7i\2\2\u00d7\u00d8"+
		"\7p\2\2\u00d8\u00d9\7q\2\2\u00d9\u00da\7t\2\2\u00da\u00db\7g\2\2\u00db"+
		".\3\2\2\2\u00dc\u00dd\7c\2\2\u00dd\u00de\7p\2\2\u00de\u00df\7f\2\2\u00df"+
		"\60\3\2\2\2\u00e0\u00e1\7?\2\2\u00e1\62\3\2\2\2\u00e2\u00e8\7)\2\2\u00e3"+
		"\u00e7\n\2\2\2\u00e4\u00e5\7^\2\2\u00e5\u00e7\13\2\2\2\u00e6\u00e3\3\2"+
		"\2\2\u00e6\u00e4\3\2\2\2\u00e7\u00ea\3\2\2\2\u00e8\u00e6\3\2\2\2\u00e8"+
		"\u00e9\3\2\2\2\u00e9\u00eb\3\2\2\2\u00ea\u00e8\3\2\2\2\u00eb\u00f7\7)"+
		"\2\2\u00ec\u00f2\7$\2\2\u00ed\u00f1\n\3\2\2\u00ee\u00ef\7^\2\2\u00ef\u00f1"+
		"\13\2\2\2\u00f0\u00ed\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f1\u00f4\3\2\2\2"+
		"\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f5\3\2\2\2\u00f4\u00f2"+
		"\3\2\2\2\u00f5\u00f7\7$\2\2\u00f6\u00e2\3\2\2\2\u00f6\u00ec\3\2\2\2\u00f7"+
		"\64\3\2\2\2\u00f8\u00fc\5;\36\2\u00f9\u00fc\59\35\2\u00fa\u00fc\7a\2\2"+
		"\u00fb\u00f8\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fb\u00fa\3\2\2\2\u00fc\u00fd"+
		"\3\2\2\2\u00fd\u00fb\3\2\2\2\u00fd\u00fe\3\2\2\2\u00fe\66\3\2\2\2\u00ff"+
		"\u0105\7b\2\2\u0100\u0104\n\4\2\2\u0101\u0102\7b\2\2\u0102\u0104\7b\2"+
		"\2\u0103\u0100\3\2\2\2\u0103\u0101\3\2\2\2\u0104\u0107\3\2\2\2\u0105\u0103"+
		"\3\2\2\2\u0105\u0106\3\2\2\2\u0106\u0108\3\2\2\2\u0107\u0105\3\2\2\2\u0108"+
		"\u0109\7b\2\2\u01098\3\2\2\2\u010a\u010b\t\5\2\2\u010b:\3\2\2\2\u010c"+
		"\u010d\t\6\2\2\u010d<\3\2\2\2\u010e\u010f\7/\2\2\u010f\u0110\7/\2\2\u0110"+
		"\u0114\3\2\2\2\u0111\u0113\n\7\2\2\u0112\u0111\3\2\2\2\u0113\u0116\3\2"+
		"\2\2\u0114\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0118\3\2\2\2\u0116"+
		"\u0114\3\2\2\2\u0117\u0119\7\17\2\2\u0118\u0117\3\2\2\2\u0118\u0119\3"+
		"\2\2\2\u0119\u011b\3\2\2\2\u011a\u011c\7\f\2\2\u011b\u011a\3\2\2\2\u011b"+
		"\u011c\3\2\2\2\u011c\u011d\3\2\2\2\u011d\u011e\b\37\2\2\u011e>\3\2\2\2"+
		"\u011f\u0120\7\61\2\2\u0120\u0121\7,\2\2\u0121\u0122\7,\2\2\u0122\u0123"+
		"\7\61\2\2\u0123\u0124\3\2\2\2\u0124\u0125\b \2\2\u0125@\3\2\2\2\u0126"+
		"\u0127\7\61\2\2\u0127\u0128\7,\2\2\u0128\u0129\3\2\2\2\u0129\u012d\n\b"+
		"\2\2\u012a\u012c\13\2\2\2\u012b\u012a\3\2\2\2\u012c\u012f\3\2\2\2\u012d"+
		"\u012e\3\2\2\2\u012d\u012b\3\2\2\2\u012e\u0130\3\2\2\2\u012f\u012d\3\2"+
		"\2\2\u0130\u0131\7,\2\2\u0131\u0132\7\61\2\2\u0132\u0133\3\2\2\2\u0133"+
		"\u0134\b!\2\2\u0134B\3\2\2\2\u0135\u0137\t\t\2\2\u0136\u0135\3\2\2\2\u0137"+
		"\u0138\3\2\2\2\u0138\u0136\3\2\2\2\u0138\u0139\3\2\2\2\u0139\u013a\3\2"+
		"\2\2\u013a\u013b\b\"\2\2\u013bD\3\2\2\2\u013c\u013d\13\2\2\2\u013dF\3"+
		"\2\2\2\21\2\u00e6\u00e8\u00f0\u00f2\u00f6\u00fb\u00fd\u0103\u0105\u0114"+
		"\u0118\u011b\u012d\u0138\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}