// Generated from DSLSQL.g4 by ANTLR 4.7.1

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
	static { RuntimeMetaData.checkVersion("4.7.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, STRING=29, BLOCK_STRING=30, IDENTIFIER=31, 
		BACKQUOTED_IDENTIFIER=32, SIMPLE_COMMENT=33, BRACKETED_EMPTY_COMMENT=34, 
		BRACKETED_COMMENT=35, WS=36, UNRECOGNIZED=37;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "T__24", 
		"T__25", "T__26", "T__27", "STRING", "BLOCK_STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"DIGIT", "LETTER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", 
		"WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'options'", "'where'", "'as'", "'save'", "'partitionBy'", 
		"'partitionby'", "'select'", "';'", "'insert'", "'create'", "'drop'", 
		"'refresh'", "'set'", "'='", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'overwrite'", "'append'", 
		"'errorIfExists'", "'ignore'", "'and'", null, null, null, null, null, 
		"'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, "STRING", "BLOCK_STRING", "IDENTIFIER", 
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
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\'\u017b\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\3\2\3\2\3\2\3\2\3\2"+
		"\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3"+
		"\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3"+
		"\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\20\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25"+
		"\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27"+
		"\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30"+
		"\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31"+
		"\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33"+
		"\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34"+
		"\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\7\36\u0115\n\36\f\36\16\36\u0118"+
		"\13\36\3\36\3\36\3\36\3\36\3\36\7\36\u011f\n\36\f\36\16\36\u0122\13\36"+
		"\3\36\5\36\u0125\n\36\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u012d\n\37\f"+
		"\37\16\37\u0130\13\37\3\37\3\37\3\37\3\37\3 \3 \3 \6 \u0139\n \r \16 "+
		"\u013a\3!\3!\3!\3!\7!\u0141\n!\f!\16!\u0144\13!\3!\3!\3\"\3\"\3#\3#\3"+
		"$\3$\3$\3$\7$\u0150\n$\f$\16$\u0153\13$\3$\5$\u0156\n$\3$\5$\u0159\n$"+
		"\3$\3$\3%\3%\3%\3%\3%\3%\3%\3&\3&\3&\3&\3&\7&\u0169\n&\f&\16&\u016c\13"+
		"&\3&\3&\3&\3&\3&\3\'\6\'\u0174\n\'\r\'\16\'\u0175\3\'\3\'\3(\3(\4\u012e"+
		"\u016a\2)\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36;\37= ?!A\"C\2E\2G#I$K%M&O\'\3\2\n\4\2))^^\4\2$$^^\3\2--\3\2bb"+
		"\3\2\62;\4\2C\\c|\4\2\f\f\17\17\5\2\13\f\17\17\"\"\2\u0188\2\3\3\2\2\2"+
		"\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2"+
		"\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2"+
		"\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2"+
		"\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2"+
		"\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2"+
		"\2\2\2?\3\2\2\2\2A\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2"+
		"\2O\3\2\2\2\3Q\3\2\2\2\5V\3\2\2\2\7X\3\2\2\2\t`\3\2\2\2\13f\3\2\2\2\r"+
		"i\3\2\2\2\17n\3\2\2\2\21z\3\2\2\2\23\u0086\3\2\2\2\25\u008d\3\2\2\2\27"+
		"\u008f\3\2\2\2\31\u0096\3\2\2\2\33\u009d\3\2\2\2\35\u00a2\3\2\2\2\37\u00aa"+
		"\3\2\2\2!\u00ae\3\2\2\2#\u00b0\3\2\2\2%\u00b8\3\2\2\2\'\u00be\3\2\2\2"+
		")\u00c2\3\2\2\2+\u00ca\3\2\2\2-\u00d3\3\2\2\2/\u00de\3\2\2\2\61\u00e6"+
		"\3\2\2\2\63\u00f0\3\2\2\2\65\u00f7\3\2\2\2\67\u0105\3\2\2\29\u010c\3\2"+
		"\2\2;\u0124\3\2\2\2=\u0126\3\2\2\2?\u0138\3\2\2\2A\u013c\3\2\2\2C\u0147"+
		"\3\2\2\2E\u0149\3\2\2\2G\u014b\3\2\2\2I\u015c\3\2\2\2K\u0163\3\2\2\2M"+
		"\u0173\3\2\2\2O\u0179\3\2\2\2QR\7n\2\2RS\7q\2\2ST\7c\2\2TU\7f\2\2U\4\3"+
		"\2\2\2VW\7\60\2\2W\6\3\2\2\2XY\7q\2\2YZ\7r\2\2Z[\7v\2\2[\\\7k\2\2\\]\7"+
		"q\2\2]^\7p\2\2^_\7u\2\2_\b\3\2\2\2`a\7y\2\2ab\7j\2\2bc\7g\2\2cd\7t\2\2"+
		"de\7g\2\2e\n\3\2\2\2fg\7c\2\2gh\7u\2\2h\f\3\2\2\2ij\7u\2\2jk\7c\2\2kl"+
		"\7x\2\2lm\7g\2\2m\16\3\2\2\2no\7r\2\2op\7c\2\2pq\7t\2\2qr\7v\2\2rs\7k"+
		"\2\2st\7v\2\2tu\7k\2\2uv\7q\2\2vw\7p\2\2wx\7D\2\2xy\7{\2\2y\20\3\2\2\2"+
		"z{\7r\2\2{|\7c\2\2|}\7t\2\2}~\7v\2\2~\177\7k\2\2\177\u0080\7v\2\2\u0080"+
		"\u0081\7k\2\2\u0081\u0082\7q\2\2\u0082\u0083\7p\2\2\u0083\u0084\7d\2\2"+
		"\u0084\u0085\7{\2\2\u0085\22\3\2\2\2\u0086\u0087\7u\2\2\u0087\u0088\7"+
		"g\2\2\u0088\u0089\7n\2\2\u0089\u008a\7g\2\2\u008a\u008b\7e\2\2\u008b\u008c"+
		"\7v\2\2\u008c\24\3\2\2\2\u008d\u008e\7=\2\2\u008e\26\3\2\2\2\u008f\u0090"+
		"\7k\2\2\u0090\u0091\7p\2\2\u0091\u0092\7u\2\2\u0092\u0093\7g\2\2\u0093"+
		"\u0094\7t\2\2\u0094\u0095\7v\2\2\u0095\30\3\2\2\2\u0096\u0097\7e\2\2\u0097"+
		"\u0098\7t\2\2\u0098\u0099\7g\2\2\u0099\u009a\7c\2\2\u009a\u009b\7v\2\2"+
		"\u009b\u009c\7g\2\2\u009c\32\3\2\2\2\u009d\u009e\7f\2\2\u009e\u009f\7"+
		"t\2\2\u009f\u00a0\7q\2\2\u00a0\u00a1\7r\2\2\u00a1\34\3\2\2\2\u00a2\u00a3"+
		"\7t\2\2\u00a3\u00a4\7g\2\2\u00a4\u00a5\7h\2\2\u00a5\u00a6\7t\2\2\u00a6"+
		"\u00a7\7g\2\2\u00a7\u00a8\7u\2\2\u00a8\u00a9\7j\2\2\u00a9\36\3\2\2\2\u00aa"+
		"\u00ab\7u\2\2\u00ab\u00ac\7g\2\2\u00ac\u00ad\7v\2\2\u00ad \3\2\2\2\u00ae"+
		"\u00af\7?\2\2\u00af\"\3\2\2\2\u00b0\u00b1\7e\2\2\u00b1\u00b2\7q\2\2\u00b2"+
		"\u00b3\7p\2\2\u00b3\u00b4\7p\2\2\u00b4\u00b5\7g\2\2\u00b5\u00b6\7e\2\2"+
		"\u00b6\u00b7\7v\2\2\u00b7$\3\2\2\2\u00b8\u00b9\7v\2\2\u00b9\u00ba\7t\2"+
		"\2\u00ba\u00bb\7c\2\2\u00bb\u00bc\7k\2\2\u00bc\u00bd\7p\2\2\u00bd&\3\2"+
		"\2\2\u00be\u00bf\7t\2\2\u00bf\u00c0\7w\2\2\u00c0\u00c1\7p\2\2\u00c1(\3"+
		"\2\2\2\u00c2\u00c3\7r\2\2\u00c3\u00c4\7t\2\2\u00c4\u00c5\7g\2\2\u00c5"+
		"\u00c6\7f\2\2\u00c6\u00c7\7k\2\2\u00c7\u00c8\7e\2\2\u00c8\u00c9\7v\2\2"+
		"\u00c9*\3\2\2\2\u00ca\u00cb\7t\2\2\u00cb\u00cc\7g\2\2\u00cc\u00cd\7i\2"+
		"\2\u00cd\u00ce\7k\2\2\u00ce\u00cf\7u\2\2\u00cf\u00d0\7v\2\2\u00d0\u00d1"+
		"\7g\2\2\u00d1\u00d2\7t\2\2\u00d2,\3\2\2\2\u00d3\u00d4\7w\2\2\u00d4\u00d5"+
		"\7p\2\2\u00d5\u00d6\7t\2\2\u00d6\u00d7\7g\2\2\u00d7\u00d8\7i\2\2\u00d8"+
		"\u00d9\7k\2\2\u00d9\u00da\7u\2\2\u00da\u00db\7v\2\2\u00db\u00dc\7g\2\2"+
		"\u00dc\u00dd\7t\2\2\u00dd.\3\2\2\2\u00de\u00df\7k\2\2\u00df\u00e0\7p\2"+
		"\2\u00e0\u00e1\7e\2\2\u00e1\u00e2\7n\2\2\u00e2\u00e3\7w\2\2\u00e3\u00e4"+
		"\7f\2\2\u00e4\u00e5\7g\2\2\u00e5\60\3\2\2\2\u00e6\u00e7\7q\2\2\u00e7\u00e8"+
		"\7x\2\2\u00e8\u00e9\7g\2\2\u00e9\u00ea\7t\2\2\u00ea\u00eb\7y\2\2\u00eb"+
		"\u00ec\7t\2\2\u00ec\u00ed\7k\2\2\u00ed\u00ee\7v\2\2\u00ee\u00ef\7g\2\2"+
		"\u00ef\62\3\2\2\2\u00f0\u00f1\7c\2\2\u00f1\u00f2\7r\2\2\u00f2\u00f3\7"+
		"r\2\2\u00f3\u00f4\7g\2\2\u00f4\u00f5\7p\2\2\u00f5\u00f6\7f\2\2\u00f6\64"+
		"\3\2\2\2\u00f7\u00f8\7g\2\2\u00f8\u00f9\7t\2\2\u00f9\u00fa\7t\2\2\u00fa"+
		"\u00fb\7q\2\2\u00fb\u00fc\7t\2\2\u00fc\u00fd\7K\2\2\u00fd\u00fe\7h\2\2"+
		"\u00fe\u00ff\7G\2\2\u00ff\u0100\7z\2\2\u0100\u0101\7k\2\2\u0101\u0102"+
		"\7u\2\2\u0102\u0103\7v\2\2\u0103\u0104\7u\2\2\u0104\66\3\2\2\2\u0105\u0106"+
		"\7k\2\2\u0106\u0107\7i\2\2\u0107\u0108\7p\2\2\u0108\u0109\7q\2\2\u0109"+
		"\u010a\7t\2\2\u010a\u010b\7g\2\2\u010b8\3\2\2\2\u010c\u010d\7c\2\2\u010d"+
		"\u010e\7p\2\2\u010e\u010f\7f\2\2\u010f:\3\2\2\2\u0110\u0116\7)\2\2\u0111"+
		"\u0115\n\2\2\2\u0112\u0113\7^\2\2\u0113\u0115\13\2\2\2\u0114\u0111\3\2"+
		"\2\2\u0114\u0112\3\2\2\2\u0115\u0118\3\2\2\2\u0116\u0114\3\2\2\2\u0116"+
		"\u0117\3\2\2\2\u0117\u0119\3\2\2\2\u0118\u0116\3\2\2\2\u0119\u0125\7)"+
		"\2\2\u011a\u0120\7$\2\2\u011b\u011f\n\3\2\2\u011c\u011d\7^\2\2\u011d\u011f"+
		"\13\2\2\2\u011e\u011b\3\2\2\2\u011e\u011c\3\2\2\2\u011f\u0122\3\2\2\2"+
		"\u0120\u011e\3\2\2\2\u0120\u0121\3\2\2\2\u0121\u0123\3\2\2\2\u0122\u0120"+
		"\3\2\2\2\u0123\u0125\7$\2\2\u0124\u0110\3\2\2\2\u0124\u011a\3\2\2\2\u0125"+
		"<\3\2\2\2\u0126\u0127\7)\2\2\u0127\u0128\7)\2\2\u0128\u0129\7)\2\2\u0129"+
		"\u012a\3\2\2\2\u012a\u012e\n\4\2\2\u012b\u012d\13\2\2\2\u012c\u012b\3"+
		"\2\2\2\u012d\u0130\3\2\2\2\u012e\u012f\3\2\2\2\u012e\u012c\3\2\2\2\u012f"+
		"\u0131\3\2\2\2\u0130\u012e\3\2\2\2\u0131\u0132\7)\2\2\u0132\u0133\7)\2"+
		"\2\u0133\u0134\7)\2\2\u0134>\3\2\2\2\u0135\u0139\5E#\2\u0136\u0139\5C"+
		"\"\2\u0137\u0139\7a\2\2\u0138\u0135\3\2\2\2\u0138\u0136\3\2\2\2\u0138"+
		"\u0137\3\2\2\2\u0139\u013a\3\2\2\2\u013a\u0138\3\2\2\2\u013a\u013b\3\2"+
		"\2\2\u013b@\3\2\2\2\u013c\u0142\7b\2\2\u013d\u0141\n\5\2\2\u013e\u013f"+
		"\7b\2\2\u013f\u0141\7b\2\2\u0140\u013d\3\2\2\2\u0140\u013e\3\2\2\2\u0141"+
		"\u0144\3\2\2\2\u0142\u0140\3\2\2\2\u0142\u0143\3\2\2\2\u0143\u0145\3\2"+
		"\2\2\u0144\u0142\3\2\2\2\u0145\u0146\7b\2\2\u0146B\3\2\2\2\u0147\u0148"+
		"\t\6\2\2\u0148D\3\2\2\2\u0149\u014a\t\7\2\2\u014aF\3\2\2\2\u014b\u014c"+
		"\7/\2\2\u014c\u014d\7/\2\2\u014d\u0151\3\2\2\2\u014e\u0150\n\b\2\2\u014f"+
		"\u014e\3\2\2\2\u0150\u0153\3\2\2\2\u0151\u014f\3\2\2\2\u0151\u0152\3\2"+
		"\2\2\u0152\u0155\3\2\2\2\u0153\u0151\3\2\2\2\u0154\u0156\7\17\2\2\u0155"+
		"\u0154\3\2\2\2\u0155\u0156\3\2\2\2\u0156\u0158\3\2\2\2\u0157\u0159\7\f"+
		"\2\2\u0158\u0157\3\2\2\2\u0158\u0159\3\2\2\2\u0159\u015a\3\2\2\2\u015a"+
		"\u015b\b$\2\2\u015bH\3\2\2\2\u015c\u015d\7\61\2\2\u015d\u015e\7,\2\2\u015e"+
		"\u015f\7,\2\2\u015f\u0160\7\61\2\2\u0160\u0161\3\2\2\2\u0161\u0162\b%"+
		"\2\2\u0162J\3\2\2\2\u0163\u0164\7\61\2\2\u0164\u0165\7,\2\2\u0165\u0166"+
		"\3\2\2\2\u0166\u016a\n\4\2\2\u0167\u0169\13\2\2\2\u0168\u0167\3\2\2\2"+
		"\u0169\u016c\3\2\2\2\u016a\u016b\3\2\2\2\u016a\u0168\3\2\2\2\u016b\u016d"+
		"\3\2\2\2\u016c\u016a\3\2\2\2\u016d\u016e\7,\2\2\u016e\u016f\7\61\2\2\u016f"+
		"\u0170\3\2\2\2\u0170\u0171\b&\2\2\u0171L\3\2\2\2\u0172\u0174\t\t\2\2\u0173"+
		"\u0172\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0173\3\2\2\2\u0175\u0176\3\2"+
		"\2\2\u0176\u0177\3\2\2\2\u0177\u0178\b\'\2\2\u0178N\3\2\2\2\u0179\u017a"+
		"\13\2\2\2\u017aP\3\2\2\2\22\2\u0114\u0116\u011e\u0120\u0124\u012e\u0138"+
		"\u013a\u0140\u0142\u0151\u0155\u0158\u016a\u0175\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}