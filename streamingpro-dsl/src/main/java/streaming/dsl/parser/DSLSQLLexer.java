// Generated from /opt/projects/kyligence/byzerCP/byzer-lang/streamingpro-dsl/src/main/resources/DSLSQL.g4 by ANTLR 4.7

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
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, AS=11, INTO=12, LOAD=13, SAVE=14, SELECT=15, INSERT=16, CREATE=17, 
		DROP=18, REFRESH=19, SET=20, CONNECT=21, TRAIN=22, RUN=23, PREDICT=24, 
		REGISTER=25, UNREGISTER=26, INCLUDE=27, OPTIONS=28, WHERE=29, PARTITIONBY=30, 
		OVERWRITE=31, APPEND=32, ERRORIfExists=33, IGNORE=34, STRING=35, BLOCK_STRING=36, 
		IDENTIFIER=37, BACKQUOTED_IDENTIFIER=38, EXECUTE_COMMAND=39, EXECUTE_TOKEN=40, 
		SIMPLE_COMMENT=41, BRACKETED_EMPTY_COMMENT=42, BRACKETED_COMMENT=43, WS=44, 
		UNRECOGNIZED=45;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "AS", "INTO", "LOAD", "SAVE", "SELECT", "INSERT", "CREATE", "DROP", 
		"REFRESH", "SET", "CONNECT", "TRAIN", "RUN", "PREDICT", "REGISTER", "UNREGISTER", 
		"INCLUDE", "OPTIONS", "WHERE", "PARTITIONBY", "OVERWRITE", "APPEND", "ERRORIfExists", 
		"IGNORE", "STRING", "BLOCK_STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"DIGIT", "LETTER", "EXECUTE_COMMAND", "EXECUTE_TOKEN", "SIMPLE_COMMENT", 
		"BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'.'", "';'", "'='", "'and'", "'-'", "'/'", "'>'", "'<'", "'~'", 
		"','", "'as'", "'into'", "'load'", "'save'", "'select'", "'insert'", "'create'", 
		"'drop'", "'refresh'", "'set'", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'options'", "'where'", null, 
		"'overwrite'", "'append'", "'errorifexists'", "'ignore'", null, null, 
		null, null, null, "'!'", null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, "AS", 
		"INTO", "LOAD", "SAVE", "SELECT", "INSERT", "CREATE", "DROP", "REFRESH", 
		"SET", "CONNECT", "TRAIN", "RUN", "PREDICT", "REGISTER", "UNREGISTER", 
		"INCLUDE", "OPTIONS", "WHERE", "PARTITIONBY", "OVERWRITE", "APPEND", "ERRORIfExists", 
		"IGNORE", "STRING", "BLOCK_STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"EXECUTE_COMMAND", "EXECUTE_TOKEN", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2/\u01a6\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3"+
		"\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\r\3"+
		"\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\22"+
		"\3\22\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24"+
		"\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26"+
		"\3\26\3\26\3\26\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\31"+
		"\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32"+
		"\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\34"+
		"\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35"+
		"\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
		"\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37"+
		"\3\37\5\37\u010a\n\37\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3!\3!\3!\3!\3!\3!"+
		"\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3"+
		"#\3#\3#\3#\3$\3$\3$\3$\7$\u0136\n$\f$\16$\u0139\13$\3$\3$\3$\3$\3$\7$"+
		"\u0140\n$\f$\16$\u0143\13$\3$\5$\u0146\n$\3%\3%\3%\3%\3%\3%\7%\u014e\n"+
		"%\f%\16%\u0151\13%\3%\3%\3%\3%\3&\3&\3&\6&\u015a\n&\r&\16&\u015b\3\'\3"+
		"\'\3\'\3\'\7\'\u0162\n\'\f\'\16\'\u0165\13\'\3\'\3\'\3(\3(\3)\3)\3*\3"+
		"*\3*\3*\6*\u0171\n*\r*\16*\u0172\3+\3+\3,\3,\3,\3,\7,\u017b\n,\f,\16,"+
		"\u017e\13,\3,\5,\u0181\n,\3,\5,\u0184\n,\3,\3,\3-\3-\3-\3-\3-\3-\3-\3"+
		".\3.\3.\3.\3.\7.\u0194\n.\f.\16.\u0197\13.\3.\3.\3.\3.\3.\3/\6/\u019f"+
		"\n/\r/\16/\u01a0\3/\3/\3\60\3\60\4\u014f\u0195\2\61\3\3\5\4\7\5\t\6\13"+
		"\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'"+
		"\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'"+
		"M(O\2Q\2S)U*W+Y,[-]._/\3\2\n\4\2))^^\4\2$$^^\3\2--\3\2bb\3\2\62;\4\2C"+
		"\\c|\4\2\f\f\17\17\5\2\13\f\17\17\"\"\2\u01b7\2\3\3\2\2\2\2\5\3\2\2\2"+
		"\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3"+
		"\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2"+
		"\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2"+
		"\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2"+
		"\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2"+
		"\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2"+
		"\2M\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]"+
		"\3\2\2\2\2_\3\2\2\2\3a\3\2\2\2\5c\3\2\2\2\7e\3\2\2\2\tg\3\2\2\2\13k\3"+
		"\2\2\2\rm\3\2\2\2\17o\3\2\2\2\21q\3\2\2\2\23s\3\2\2\2\25u\3\2\2\2\27w"+
		"\3\2\2\2\31z\3\2\2\2\33\177\3\2\2\2\35\u0084\3\2\2\2\37\u0089\3\2\2\2"+
		"!\u0090\3\2\2\2#\u0097\3\2\2\2%\u009e\3\2\2\2\'\u00a3\3\2\2\2)\u00ab\3"+
		"\2\2\2+\u00af\3\2\2\2-\u00b7\3\2\2\2/\u00bd\3\2\2\2\61\u00c1\3\2\2\2\63"+
		"\u00c9\3\2\2\2\65\u00d2\3\2\2\2\67\u00dd\3\2\2\29\u00e5\3\2\2\2;\u00ed"+
		"\3\2\2\2=\u0109\3\2\2\2?\u010b\3\2\2\2A\u0115\3\2\2\2C\u011c\3\2\2\2E"+
		"\u012a\3\2\2\2G\u0145\3\2\2\2I\u0147\3\2\2\2K\u0159\3\2\2\2M\u015d\3\2"+
		"\2\2O\u0168\3\2\2\2Q\u016a\3\2\2\2S\u016c\3\2\2\2U\u0174\3\2\2\2W\u0176"+
		"\3\2\2\2Y\u0187\3\2\2\2[\u018e\3\2\2\2]\u019e\3\2\2\2_\u01a4\3\2\2\2a"+
		"b\7\60\2\2b\4\3\2\2\2cd\7=\2\2d\6\3\2\2\2ef\7?\2\2f\b\3\2\2\2gh\7c\2\2"+
		"hi\7p\2\2ij\7f\2\2j\n\3\2\2\2kl\7/\2\2l\f\3\2\2\2mn\7\61\2\2n\16\3\2\2"+
		"\2op\7@\2\2p\20\3\2\2\2qr\7>\2\2r\22\3\2\2\2st\7\u0080\2\2t\24\3\2\2\2"+
		"uv\7.\2\2v\26\3\2\2\2wx\7c\2\2xy\7u\2\2y\30\3\2\2\2z{\7k\2\2{|\7p\2\2"+
		"|}\7v\2\2}~\7q\2\2~\32\3\2\2\2\177\u0080\7n\2\2\u0080\u0081\7q\2\2\u0081"+
		"\u0082\7c\2\2\u0082\u0083\7f\2\2\u0083\34\3\2\2\2\u0084\u0085\7u\2\2\u0085"+
		"\u0086\7c\2\2\u0086\u0087\7x\2\2\u0087\u0088\7g\2\2\u0088\36\3\2\2\2\u0089"+
		"\u008a\7u\2\2\u008a\u008b\7g\2\2\u008b\u008c\7n\2\2\u008c\u008d\7g\2\2"+
		"\u008d\u008e\7e\2\2\u008e\u008f\7v\2\2\u008f \3\2\2\2\u0090\u0091\7k\2"+
		"\2\u0091\u0092\7p\2\2\u0092\u0093\7u\2\2\u0093\u0094\7g\2\2\u0094\u0095"+
		"\7t\2\2\u0095\u0096\7v\2\2\u0096\"\3\2\2\2\u0097\u0098\7e\2\2\u0098\u0099"+
		"\7t\2\2\u0099\u009a\7g\2\2\u009a\u009b\7c\2\2\u009b\u009c\7v\2\2\u009c"+
		"\u009d\7g\2\2\u009d$\3\2\2\2\u009e\u009f\7f\2\2\u009f\u00a0\7t\2\2\u00a0"+
		"\u00a1\7q\2\2\u00a1\u00a2\7r\2\2\u00a2&\3\2\2\2\u00a3\u00a4\7t\2\2\u00a4"+
		"\u00a5\7g\2\2\u00a5\u00a6\7h\2\2\u00a6\u00a7\7t\2\2\u00a7\u00a8\7g\2\2"+
		"\u00a8\u00a9\7u\2\2\u00a9\u00aa\7j\2\2\u00aa(\3\2\2\2\u00ab\u00ac\7u\2"+
		"\2\u00ac\u00ad\7g\2\2\u00ad\u00ae\7v\2\2\u00ae*\3\2\2\2\u00af\u00b0\7"+
		"e\2\2\u00b0\u00b1\7q\2\2\u00b1\u00b2\7p\2\2\u00b2\u00b3\7p\2\2\u00b3\u00b4"+
		"\7g\2\2\u00b4\u00b5\7e\2\2\u00b5\u00b6\7v\2\2\u00b6,\3\2\2\2\u00b7\u00b8"+
		"\7v\2\2\u00b8\u00b9\7t\2\2\u00b9\u00ba\7c\2\2\u00ba\u00bb\7k\2\2\u00bb"+
		"\u00bc\7p\2\2\u00bc.\3\2\2\2\u00bd\u00be\7t\2\2\u00be\u00bf\7w\2\2\u00bf"+
		"\u00c0\7p\2\2\u00c0\60\3\2\2\2\u00c1\u00c2\7r\2\2\u00c2\u00c3\7t\2\2\u00c3"+
		"\u00c4\7g\2\2\u00c4\u00c5\7f\2\2\u00c5\u00c6\7k\2\2\u00c6\u00c7\7e\2\2"+
		"\u00c7\u00c8\7v\2\2\u00c8\62\3\2\2\2\u00c9\u00ca\7t\2\2\u00ca\u00cb\7"+
		"g\2\2\u00cb\u00cc\7i\2\2\u00cc\u00cd\7k\2\2\u00cd\u00ce\7u\2\2\u00ce\u00cf"+
		"\7v\2\2\u00cf\u00d0\7g\2\2\u00d0\u00d1\7t\2\2\u00d1\64\3\2\2\2\u00d2\u00d3"+
		"\7w\2\2\u00d3\u00d4\7p\2\2\u00d4\u00d5\7t\2\2\u00d5\u00d6\7g\2\2\u00d6"+
		"\u00d7\7i\2\2\u00d7\u00d8\7k\2\2\u00d8\u00d9\7u\2\2\u00d9\u00da\7v\2\2"+
		"\u00da\u00db\7g\2\2\u00db\u00dc\7t\2\2\u00dc\66\3\2\2\2\u00dd\u00de\7"+
		"k\2\2\u00de\u00df\7p\2\2\u00df\u00e0\7e\2\2\u00e0\u00e1\7n\2\2\u00e1\u00e2"+
		"\7w\2\2\u00e2\u00e3\7f\2\2\u00e3\u00e4\7g\2\2\u00e48\3\2\2\2\u00e5\u00e6"+
		"\7q\2\2\u00e6\u00e7\7r\2\2\u00e7\u00e8\7v\2\2\u00e8\u00e9\7k\2\2\u00e9"+
		"\u00ea\7q\2\2\u00ea\u00eb\7p\2\2\u00eb\u00ec\7u\2\2\u00ec:\3\2\2\2\u00ed"+
		"\u00ee\7y\2\2\u00ee\u00ef\7j\2\2\u00ef\u00f0\7g\2\2\u00f0\u00f1\7t\2\2"+
		"\u00f1\u00f2\7g\2\2\u00f2<\3\2\2\2\u00f3\u00f4\7r\2\2\u00f4\u00f5\7c\2"+
		"\2\u00f5\u00f6\7t\2\2\u00f6\u00f7\7v\2\2\u00f7\u00f8\7k\2\2\u00f8\u00f9"+
		"\7v\2\2\u00f9\u00fa\7k\2\2\u00fa\u00fb\7q\2\2\u00fb\u00fc\7p\2\2\u00fc"+
		"\u00fd\7D\2\2\u00fd\u010a\7{\2\2\u00fe\u00ff\7r\2\2\u00ff\u0100\7c\2\2"+
		"\u0100\u0101\7t\2\2\u0101\u0102\7v\2\2\u0102\u0103\7k\2\2\u0103\u0104"+
		"\7v\2\2\u0104\u0105\7k\2\2\u0105\u0106\7q\2\2\u0106\u0107\7p\2\2\u0107"+
		"\u0108\7d\2\2\u0108\u010a\7{\2\2\u0109\u00f3\3\2\2\2\u0109\u00fe\3\2\2"+
		"\2\u010a>\3\2\2\2\u010b\u010c\7q\2\2\u010c\u010d\7x\2\2\u010d\u010e\7"+
		"g\2\2\u010e\u010f\7t\2\2\u010f\u0110\7y\2\2\u0110\u0111\7t\2\2\u0111\u0112"+
		"\7k\2\2\u0112\u0113\7v\2\2\u0113\u0114\7g\2\2\u0114@\3\2\2\2\u0115\u0116"+
		"\7c\2\2\u0116\u0117\7r\2\2\u0117\u0118\7r\2\2\u0118\u0119\7g\2\2\u0119"+
		"\u011a\7p\2\2\u011a\u011b\7f\2\2\u011bB\3\2\2\2\u011c\u011d\7g\2\2\u011d"+
		"\u011e\7t\2\2\u011e\u011f\7t\2\2\u011f\u0120\7q\2\2\u0120\u0121\7t\2\2"+
		"\u0121\u0122\7k\2\2\u0122\u0123\7h\2\2\u0123\u0124\7g\2\2\u0124\u0125"+
		"\7z\2\2\u0125\u0126\7k\2\2\u0126\u0127\7u\2\2\u0127\u0128\7v\2\2\u0128"+
		"\u0129\7u\2\2\u0129D\3\2\2\2\u012a\u012b\7k\2\2\u012b\u012c\7i\2\2\u012c"+
		"\u012d\7p\2\2\u012d\u012e\7q\2\2\u012e\u012f\7t\2\2\u012f\u0130\7g\2\2"+
		"\u0130F\3\2\2\2\u0131\u0137\7)\2\2\u0132\u0136\n\2\2\2\u0133\u0134\7^"+
		"\2\2\u0134\u0136\13\2\2\2\u0135\u0132\3\2\2\2\u0135\u0133\3\2\2\2\u0136"+
		"\u0139\3\2\2\2\u0137\u0135\3\2\2\2\u0137\u0138\3\2\2\2\u0138\u013a\3\2"+
		"\2\2\u0139\u0137\3\2\2\2\u013a\u0146\7)\2\2\u013b\u0141\7$\2\2\u013c\u0140"+
		"\n\3\2\2\u013d\u013e\7^\2\2\u013e\u0140\13\2\2\2\u013f\u013c\3\2\2\2\u013f"+
		"\u013d\3\2\2\2\u0140\u0143\3\2\2\2\u0141\u013f\3\2\2\2\u0141\u0142\3\2"+
		"\2\2\u0142\u0144\3\2\2\2\u0143\u0141\3\2\2\2\u0144\u0146\7$\2\2\u0145"+
		"\u0131\3\2\2\2\u0145\u013b\3\2\2\2\u0146H\3\2\2\2\u0147\u0148\7)\2\2\u0148"+
		"\u0149\7)\2\2\u0149\u014a\7)\2\2\u014a\u014b\3\2\2\2\u014b\u014f\n\4\2"+
		"\2\u014c\u014e\13\2\2\2\u014d\u014c\3\2\2\2\u014e\u0151\3\2\2\2\u014f"+
		"\u0150\3\2\2\2\u014f\u014d\3\2\2\2\u0150\u0152\3\2\2\2\u0151\u014f\3\2"+
		"\2\2\u0152\u0153\7)\2\2\u0153\u0154\7)\2\2\u0154\u0155\7)\2\2\u0155J\3"+
		"\2\2\2\u0156\u015a\5Q)\2\u0157\u015a\5O(\2\u0158\u015a\7a\2\2\u0159\u0156"+
		"\3\2\2\2\u0159\u0157\3\2\2\2\u0159\u0158\3\2\2\2\u015a\u015b\3\2\2\2\u015b"+
		"\u0159\3\2\2\2\u015b\u015c\3\2\2\2\u015cL\3\2\2\2\u015d\u0163\7b\2\2\u015e"+
		"\u0162\n\5\2\2\u015f\u0160\7b\2\2\u0160\u0162\7b\2\2\u0161\u015e\3\2\2"+
		"\2\u0161\u015f\3\2\2\2\u0162\u0165\3\2\2\2\u0163\u0161\3\2\2\2\u0163\u0164"+
		"\3\2\2\2\u0164\u0166\3\2\2\2\u0165\u0163\3\2\2\2\u0166\u0167\7b\2\2\u0167"+
		"N\3\2\2\2\u0168\u0169\t\6\2\2\u0169P\3\2\2\2\u016a\u016b\t\7\2\2\u016b"+
		"R\3\2\2\2\u016c\u0170\5U+\2\u016d\u0171\5Q)\2\u016e\u0171\5O(\2\u016f"+
		"\u0171\7a\2\2\u0170\u016d\3\2\2\2\u0170\u016e\3\2\2\2\u0170\u016f\3\2"+
		"\2\2\u0171\u0172\3\2\2\2\u0172\u0170\3\2\2\2\u0172\u0173\3\2\2\2\u0173"+
		"T\3\2\2\2\u0174\u0175\7#\2\2\u0175V\3\2\2\2\u0176\u0177\7/\2\2\u0177\u0178"+
		"\7/\2\2\u0178\u017c\3\2\2\2\u0179\u017b\n\b\2\2\u017a\u0179\3\2\2\2\u017b"+
		"\u017e\3\2\2\2\u017c\u017a\3\2\2\2\u017c\u017d\3\2\2\2\u017d\u0180\3\2"+
		"\2\2\u017e\u017c\3\2\2\2\u017f\u0181\7\17\2\2\u0180\u017f\3\2\2\2\u0180"+
		"\u0181\3\2\2\2\u0181\u0183\3\2\2\2\u0182\u0184\7\f\2\2\u0183\u0182\3\2"+
		"\2\2\u0183\u0184\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u0186\b,\2\2\u0186"+
		"X\3\2\2\2\u0187\u0188\7\61\2\2\u0188\u0189\7,\2\2\u0189\u018a\7,\2\2\u018a"+
		"\u018b\7\61\2\2\u018b\u018c\3\2\2\2\u018c\u018d\b-\2\2\u018dZ\3\2\2\2"+
		"\u018e\u018f\7\61\2\2\u018f\u0190\7,\2\2\u0190\u0191\3\2\2\2\u0191\u0195"+
		"\n\4\2\2\u0192\u0194\13\2\2\2\u0193\u0192\3\2\2\2\u0194\u0197\3\2\2\2"+
		"\u0195\u0196\3\2\2\2\u0195\u0193\3\2\2\2\u0196\u0198\3\2\2\2\u0197\u0195"+
		"\3\2\2\2\u0198\u0199\7,\2\2\u0199\u019a\7\61\2\2\u019a\u019b\3\2\2\2\u019b"+
		"\u019c\b.\2\2\u019c\\\3\2\2\2\u019d\u019f\t\t\2\2\u019e\u019d\3\2\2\2"+
		"\u019f\u01a0\3\2\2\2\u01a0\u019e\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a2"+
		"\3\2\2\2\u01a2\u01a3\b/\2\2\u01a3^\3\2\2\2\u01a4\u01a5\13\2\2\2\u01a5"+
		"`\3\2\2\2\25\2\u0109\u0135\u0137\u013f\u0141\u0145\u014f\u0159\u015b\u0161"+
		"\u0163\u0170\u0172\u017c\u0180\u0183\u0195\u01a0\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}