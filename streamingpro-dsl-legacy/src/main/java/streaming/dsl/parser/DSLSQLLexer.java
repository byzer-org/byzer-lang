// Generated from DSLSQL.g4 by ANTLR 4.5.3

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
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, STRING=31, 
		BLOCK_STRING=32, IDENTIFIER=33, BACKQUOTED_IDENTIFIER=34, SIMPLE_COMMENT=35, 
		BRACKETED_EMPTY_COMMENT=36, BRACKETED_COMMENT=37, WS=38, UNRECOGNIZED=39;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "T__24", 
		"T__25", "T__26", "T__27", "T__28", "T__29", "STRING", "BLOCK_STRING", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", 
		"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'options'", "'where'", "'as'", "'save'", "'partitionBy'", 
		"'partitionby'", "'select'", "';'", "'insert'", "'create'", "'drop'", 
		"'refresh'", "'set'", "'='", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'overwrite'", "'append'", 
		"'errorIfExists'", "'ignore'", "'and'", "'!'", "','", null, null, null, 
		null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, "STRING", "BLOCK_STRING", "IDENTIFIER", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2)\u0183\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\3\2\3\2"+
		"\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3"+
		"\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b"+
		"\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3"+
		"\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\3\20\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3"+
		"\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\25\3"+
		"\25\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3"+
		"\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3"+
		"\30\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3"+
		"\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3"+
		"\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3"+
		"\34\3\34\3\34\3\35\3\35\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \3 \7 \u011d"+
		"\n \f \16 \u0120\13 \3 \3 \3 \3 \3 \7 \u0127\n \f \16 \u012a\13 \3 \5"+
		" \u012d\n \3!\3!\3!\3!\3!\3!\7!\u0135\n!\f!\16!\u0138\13!\3!\3!\3!\3!"+
		"\3\"\3\"\3\"\6\"\u0141\n\"\r\"\16\"\u0142\3#\3#\3#\3#\7#\u0149\n#\f#\16"+
		"#\u014c\13#\3#\3#\3$\3$\3%\3%\3&\3&\3&\3&\7&\u0158\n&\f&\16&\u015b\13"+
		"&\3&\5&\u015e\n&\3&\5&\u0161\n&\3&\3&\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3(\3"+
		"(\3(\3(\3(\7(\u0171\n(\f(\16(\u0174\13(\3(\3(\3(\3(\3(\3)\6)\u017c\n)"+
		"\r)\16)\u017d\3)\3)\3*\3*\4\u0136\u0172\2+\3\3\5\4\7\5\t\6\13\7\r\b\17"+
		"\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+"+
		"\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G\2I\2K%M&O\'Q("+
		"S)\3\2\n\4\2))^^\4\2$$^^\3\2--\3\2bb\3\2\62;\4\2C\\c|\4\2\f\f\17\17\5"+
		"\2\13\f\17\17\"\"\u0190\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2"+
		"\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25"+
		"\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2"+
		"\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2"+
		"\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3"+
		"\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2"+
		"\2\2E\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\3"+
		"U\3\2\2\2\5Z\3\2\2\2\7\\\3\2\2\2\td\3\2\2\2\13j\3\2\2\2\rm\3\2\2\2\17"+
		"r\3\2\2\2\21~\3\2\2\2\23\u008a\3\2\2\2\25\u0091\3\2\2\2\27\u0093\3\2\2"+
		"\2\31\u009a\3\2\2\2\33\u00a1\3\2\2\2\35\u00a6\3\2\2\2\37\u00ae\3\2\2\2"+
		"!\u00b2\3\2\2\2#\u00b4\3\2\2\2%\u00bc\3\2\2\2\'\u00c2\3\2\2\2)\u00c6\3"+
		"\2\2\2+\u00ce\3\2\2\2-\u00d7\3\2\2\2/\u00e2\3\2\2\2\61\u00ea\3\2\2\2\63"+
		"\u00f4\3\2\2\2\65\u00fb\3\2\2\2\67\u0109\3\2\2\29\u0110\3\2\2\2;\u0114"+
		"\3\2\2\2=\u0116\3\2\2\2?\u012c\3\2\2\2A\u012e\3\2\2\2C\u0140\3\2\2\2E"+
		"\u0144\3\2\2\2G\u014f\3\2\2\2I\u0151\3\2\2\2K\u0153\3\2\2\2M\u0164\3\2"+
		"\2\2O\u016b\3\2\2\2Q\u017b\3\2\2\2S\u0181\3\2\2\2UV\7n\2\2VW\7q\2\2WX"+
		"\7c\2\2XY\7f\2\2Y\4\3\2\2\2Z[\7\60\2\2[\6\3\2\2\2\\]\7q\2\2]^\7r\2\2^"+
		"_\7v\2\2_`\7k\2\2`a\7q\2\2ab\7p\2\2bc\7u\2\2c\b\3\2\2\2de\7y\2\2ef\7j"+
		"\2\2fg\7g\2\2gh\7t\2\2hi\7g\2\2i\n\3\2\2\2jk\7c\2\2kl\7u\2\2l\f\3\2\2"+
		"\2mn\7u\2\2no\7c\2\2op\7x\2\2pq\7g\2\2q\16\3\2\2\2rs\7r\2\2st\7c\2\2t"+
		"u\7t\2\2uv\7v\2\2vw\7k\2\2wx\7v\2\2xy\7k\2\2yz\7q\2\2z{\7p\2\2{|\7D\2"+
		"\2|}\7{\2\2}\20\3\2\2\2~\177\7r\2\2\177\u0080\7c\2\2\u0080\u0081\7t\2"+
		"\2\u0081\u0082\7v\2\2\u0082\u0083\7k\2\2\u0083\u0084\7v\2\2\u0084\u0085"+
		"\7k\2\2\u0085\u0086\7q\2\2\u0086\u0087\7p\2\2\u0087\u0088\7d\2\2\u0088"+
		"\u0089\7{\2\2\u0089\22\3\2\2\2\u008a\u008b\7u\2\2\u008b\u008c\7g\2\2\u008c"+
		"\u008d\7n\2\2\u008d\u008e\7g\2\2\u008e\u008f\7e\2\2\u008f\u0090\7v\2\2"+
		"\u0090\24\3\2\2\2\u0091\u0092\7=\2\2\u0092\26\3\2\2\2\u0093\u0094\7k\2"+
		"\2\u0094\u0095\7p\2\2\u0095\u0096\7u\2\2\u0096\u0097\7g\2\2\u0097\u0098"+
		"\7t\2\2\u0098\u0099\7v\2\2\u0099\30\3\2\2\2\u009a\u009b\7e\2\2\u009b\u009c"+
		"\7t\2\2\u009c\u009d\7g\2\2\u009d\u009e\7c\2\2\u009e\u009f\7v\2\2\u009f"+
		"\u00a0\7g\2\2\u00a0\32\3\2\2\2\u00a1\u00a2\7f\2\2\u00a2\u00a3\7t\2\2\u00a3"+
		"\u00a4\7q\2\2\u00a4\u00a5\7r\2\2\u00a5\34\3\2\2\2\u00a6\u00a7\7t\2\2\u00a7"+
		"\u00a8\7g\2\2\u00a8\u00a9\7h\2\2\u00a9\u00aa\7t\2\2\u00aa\u00ab\7g\2\2"+
		"\u00ab\u00ac\7u\2\2\u00ac\u00ad\7j\2\2\u00ad\36\3\2\2\2\u00ae\u00af\7"+
		"u\2\2\u00af\u00b0\7g\2\2\u00b0\u00b1\7v\2\2\u00b1 \3\2\2\2\u00b2\u00b3"+
		"\7?\2\2\u00b3\"\3\2\2\2\u00b4\u00b5\7e\2\2\u00b5\u00b6\7q\2\2\u00b6\u00b7"+
		"\7p\2\2\u00b7\u00b8\7p\2\2\u00b8\u00b9\7g\2\2\u00b9\u00ba\7e\2\2\u00ba"+
		"\u00bb\7v\2\2\u00bb$\3\2\2\2\u00bc\u00bd\7v\2\2\u00bd\u00be\7t\2\2\u00be"+
		"\u00bf\7c\2\2\u00bf\u00c0\7k\2\2\u00c0\u00c1\7p\2\2\u00c1&\3\2\2\2\u00c2"+
		"\u00c3\7t\2\2\u00c3\u00c4\7w\2\2\u00c4\u00c5\7p\2\2\u00c5(\3\2\2\2\u00c6"+
		"\u00c7\7r\2\2\u00c7\u00c8\7t\2\2\u00c8\u00c9\7g\2\2\u00c9\u00ca\7f\2\2"+
		"\u00ca\u00cb\7k\2\2\u00cb\u00cc\7e\2\2\u00cc\u00cd\7v\2\2\u00cd*\3\2\2"+
		"\2\u00ce\u00cf\7t\2\2\u00cf\u00d0\7g\2\2\u00d0\u00d1\7i\2\2\u00d1\u00d2"+
		"\7k\2\2\u00d2\u00d3\7u\2\2\u00d3\u00d4\7v\2\2\u00d4\u00d5\7g\2\2\u00d5"+
		"\u00d6\7t\2\2\u00d6,\3\2\2\2\u00d7\u00d8\7w\2\2\u00d8\u00d9\7p\2\2\u00d9"+
		"\u00da\7t\2\2\u00da\u00db\7g\2\2\u00db\u00dc\7i\2\2\u00dc\u00dd\7k\2\2"+
		"\u00dd\u00de\7u\2\2\u00de\u00df\7v\2\2\u00df\u00e0\7g\2\2\u00e0\u00e1"+
		"\7t\2\2\u00e1.\3\2\2\2\u00e2\u00e3\7k\2\2\u00e3\u00e4\7p\2\2\u00e4\u00e5"+
		"\7e\2\2\u00e5\u00e6\7n\2\2\u00e6\u00e7\7w\2\2\u00e7\u00e8\7f\2\2\u00e8"+
		"\u00e9\7g\2\2\u00e9\60\3\2\2\2\u00ea\u00eb\7q\2\2\u00eb\u00ec\7x\2\2\u00ec"+
		"\u00ed\7g\2\2\u00ed\u00ee\7t\2\2\u00ee\u00ef\7y\2\2\u00ef\u00f0\7t\2\2"+
		"\u00f0\u00f1\7k\2\2\u00f1\u00f2\7v\2\2\u00f2\u00f3\7g\2\2\u00f3\62\3\2"+
		"\2\2\u00f4\u00f5\7c\2\2\u00f5\u00f6\7r\2\2\u00f6\u00f7\7r\2\2\u00f7\u00f8"+
		"\7g\2\2\u00f8\u00f9\7p\2\2\u00f9\u00fa\7f\2\2\u00fa\64\3\2\2\2\u00fb\u00fc"+
		"\7g\2\2\u00fc\u00fd\7t\2\2\u00fd\u00fe\7t\2\2\u00fe\u00ff\7q\2\2\u00ff"+
		"\u0100\7t\2\2\u0100\u0101\7K\2\2\u0101\u0102\7h\2\2\u0102\u0103\7G\2\2"+
		"\u0103\u0104\7z\2\2\u0104\u0105\7k\2\2\u0105\u0106\7u\2\2\u0106\u0107"+
		"\7v\2\2\u0107\u0108\7u\2\2\u0108\66\3\2\2\2\u0109\u010a\7k\2\2\u010a\u010b"+
		"\7i\2\2\u010b\u010c\7p\2\2\u010c\u010d\7q\2\2\u010d\u010e\7t\2\2\u010e"+
		"\u010f\7g\2\2\u010f8\3\2\2\2\u0110\u0111\7c\2\2\u0111\u0112\7p\2\2\u0112"+
		"\u0113\7f\2\2\u0113:\3\2\2\2\u0114\u0115\7#\2\2\u0115<\3\2\2\2\u0116\u0117"+
		"\7.\2\2\u0117>\3\2\2\2\u0118\u011e\7)\2\2\u0119\u011d\n\2\2\2\u011a\u011b"+
		"\7^\2\2\u011b\u011d\13\2\2\2\u011c\u0119\3\2\2\2\u011c\u011a\3\2\2\2\u011d"+
		"\u0120\3\2\2\2\u011e\u011c\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\3\2"+
		"\2\2\u0120\u011e\3\2\2\2\u0121\u012d\7)\2\2\u0122\u0128\7$\2\2\u0123\u0127"+
		"\n\3\2\2\u0124\u0125\7^\2\2\u0125\u0127\13\2\2\2\u0126\u0123\3\2\2\2\u0126"+
		"\u0124\3\2\2\2\u0127\u012a\3\2\2\2\u0128\u0126\3\2\2\2\u0128\u0129\3\2"+
		"\2\2\u0129\u012b\3\2\2\2\u012a\u0128\3\2\2\2\u012b\u012d\7$\2\2\u012c"+
		"\u0118\3\2\2\2\u012c\u0122\3\2\2\2\u012d@\3\2\2\2\u012e\u012f\7)\2\2\u012f"+
		"\u0130\7)\2\2\u0130\u0131\7)\2\2\u0131\u0132\3\2\2\2\u0132\u0136\n\4\2"+
		"\2\u0133\u0135\13\2\2\2\u0134\u0133\3\2\2\2\u0135\u0138\3\2\2\2\u0136"+
		"\u0137\3\2\2\2\u0136\u0134\3\2\2\2\u0137\u0139\3\2\2\2\u0138\u0136\3\2"+
		"\2\2\u0139\u013a\7)\2\2\u013a\u013b\7)\2\2\u013b\u013c\7)\2\2\u013cB\3"+
		"\2\2\2\u013d\u0141\5I%\2\u013e\u0141\5G$\2\u013f\u0141\7a\2\2\u0140\u013d"+
		"\3\2\2\2\u0140\u013e\3\2\2\2\u0140\u013f\3\2\2\2\u0141\u0142\3\2\2\2\u0142"+
		"\u0140\3\2\2\2\u0142\u0143\3\2\2\2\u0143D\3\2\2\2\u0144\u014a\7b\2\2\u0145"+
		"\u0149\n\5\2\2\u0146\u0147\7b\2\2\u0147\u0149\7b\2\2\u0148\u0145\3\2\2"+
		"\2\u0148\u0146\3\2\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014a\u014b"+
		"\3\2\2\2\u014b\u014d\3\2\2\2\u014c\u014a\3\2\2\2\u014d\u014e\7b\2\2\u014e"+
		"F\3\2\2\2\u014f\u0150\t\6\2\2\u0150H\3\2\2\2\u0151\u0152\t\7\2\2\u0152"+
		"J\3\2\2\2\u0153\u0154\7/\2\2\u0154\u0155\7/\2\2\u0155\u0159\3\2\2\2\u0156"+
		"\u0158\n\b\2\2\u0157\u0156\3\2\2\2\u0158\u015b\3\2\2\2\u0159\u0157\3\2"+
		"\2\2\u0159\u015a\3\2\2\2\u015a\u015d\3\2\2\2\u015b\u0159\3\2\2\2\u015c"+
		"\u015e\7\17\2\2\u015d\u015c\3\2\2\2\u015d\u015e\3\2\2\2\u015e\u0160\3"+
		"\2\2\2\u015f\u0161\7\f\2\2\u0160\u015f\3\2\2\2\u0160\u0161\3\2\2\2\u0161"+
		"\u0162\3\2\2\2\u0162\u0163\b&\2\2\u0163L\3\2\2\2\u0164\u0165\7\61\2\2"+
		"\u0165\u0166\7,\2\2\u0166\u0167\7,\2\2\u0167\u0168\7\61\2\2\u0168\u0169"+
		"\3\2\2\2\u0169\u016a\b\'\2\2\u016aN\3\2\2\2\u016b\u016c\7\61\2\2\u016c"+
		"\u016d\7,\2\2\u016d\u016e\3\2\2\2\u016e\u0172\n\4\2\2\u016f\u0171\13\2"+
		"\2\2\u0170\u016f\3\2\2\2\u0171\u0174\3\2\2\2\u0172\u0173\3\2\2\2\u0172"+
		"\u0170\3\2\2\2\u0173\u0175\3\2\2\2\u0174\u0172\3\2\2\2\u0175\u0176\7,"+
		"\2\2\u0176\u0177\7\61\2\2\u0177\u0178\3\2\2\2\u0178\u0179\b(\2\2\u0179"+
		"P\3\2\2\2\u017a\u017c\t\t\2\2\u017b\u017a\3\2\2\2\u017c\u017d\3\2\2\2"+
		"\u017d\u017b\3\2\2\2\u017d\u017e\3\2\2\2\u017e\u017f\3\2\2\2\u017f\u0180"+
		"\b)\2\2\u0180R\3\2\2\2\u0181\u0182\13\2\2\2\u0182T\3\2\2\2\22\2\u011c"+
		"\u011e\u0126\u0128\u012c\u0136\u0140\u0142\u0148\u014a\u0159\u015d\u0160"+
		"\u0172\u017d\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}