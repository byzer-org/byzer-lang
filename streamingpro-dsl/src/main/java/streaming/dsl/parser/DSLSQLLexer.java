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
		T__24=25, T__25=26, STRING=27, IDENTIFIER=28, BACKQUOTED_IDENTIFIER=29, 
		SIMPLE_COMMENT=30, BRACKETED_EMPTY_COMMENT=31, BRACKETED_COMMENT=32, WS=33, 
		UNRECOGNIZED=34;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "T__18", "T__19", "T__20", "T__21", "T__22", "T__23", "T__24", 
		"T__25", "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", 
		"SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", 
		"UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'LOAD'", "'.'", "'options'", "'as'", "'save'", "'SAVE'", 
		"'partitionBy'", "'select'", "'SELECT'", "';'", "'insert'", "'INSERT'", 
		"'create'", "'CREATE'", "'set'", "'SET'", "'connect'", "'CONNECT'", "'where'", 
		"'overwrite'", "'append'", "'errorIfExists'", "'ignore'", "'and'", "'='", 
		null, null, null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2$\u014a\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3"+
		"\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3"+
		"\7\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3"+
		"\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21"+
		"\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\25"+
		"\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3\27"+
		"\3\27\3\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
		"\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32"+
		"\3\32\3\33\3\33\3\34\3\34\3\34\3\34\7\34\u00f3\n\34\f\34\16\34\u00f6\13"+
		"\34\3\34\3\34\3\34\3\34\3\34\7\34\u00fd\n\34\f\34\16\34\u0100\13\34\3"+
		"\34\5\34\u0103\n\34\3\35\3\35\3\35\6\35\u0108\n\35\r\35\16\35\u0109\3"+
		"\36\3\36\3\36\3\36\7\36\u0110\n\36\f\36\16\36\u0113\13\36\3\36\3\36\3"+
		"\37\3\37\3 \3 \3!\3!\3!\3!\7!\u011f\n!\f!\16!\u0122\13!\3!\5!\u0125\n"+
		"!\3!\5!\u0128\n!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3#\7#\u0138"+
		"\n#\f#\16#\u013b\13#\3#\3#\3#\3#\3#\3$\6$\u0143\n$\r$\16$\u0144\3$\3$"+
		"\3%\3%\3\u0139\2&\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31"+
		"\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65"+
		"\34\67\359\36;\37=\2?\2A C!E\"G#I$\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62"+
		";\4\2C\\c|\4\2\f\f\17\17\3\2--\5\2\13\f\17\17\"\"\u0156\2\3\3\2\2\2\2"+
		"\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2"+
		"\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2"+
		"\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2"+
		"\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2"+
		"\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2A\3\2\2"+
		"\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\3K\3\2\2\2\5P\3\2\2\2\7"+
		"U\3\2\2\2\tW\3\2\2\2\13_\3\2\2\2\rb\3\2\2\2\17g\3\2\2\2\21l\3\2\2\2\23"+
		"x\3\2\2\2\25\177\3\2\2\2\27\u0086\3\2\2\2\31\u0088\3\2\2\2\33\u008f\3"+
		"\2\2\2\35\u0096\3\2\2\2\37\u009d\3\2\2\2!\u00a4\3\2\2\2#\u00a8\3\2\2\2"+
		"%\u00ac\3\2\2\2\'\u00b4\3\2\2\2)\u00bc\3\2\2\2+\u00c2\3\2\2\2-\u00cc\3"+
		"\2\2\2/\u00d3\3\2\2\2\61\u00e1\3\2\2\2\63\u00e8\3\2\2\2\65\u00ec\3\2\2"+
		"\2\67\u0102\3\2\2\29\u0107\3\2\2\2;\u010b\3\2\2\2=\u0116\3\2\2\2?\u0118"+
		"\3\2\2\2A\u011a\3\2\2\2C\u012b\3\2\2\2E\u0132\3\2\2\2G\u0142\3\2\2\2I"+
		"\u0148\3\2\2\2KL\7n\2\2LM\7q\2\2MN\7c\2\2NO\7f\2\2O\4\3\2\2\2PQ\7N\2\2"+
		"QR\7Q\2\2RS\7C\2\2ST\7F\2\2T\6\3\2\2\2UV\7\60\2\2V\b\3\2\2\2WX\7q\2\2"+
		"XY\7r\2\2YZ\7v\2\2Z[\7k\2\2[\\\7q\2\2\\]\7p\2\2]^\7u\2\2^\n\3\2\2\2_`"+
		"\7c\2\2`a\7u\2\2a\f\3\2\2\2bc\7u\2\2cd\7c\2\2de\7x\2\2ef\7g\2\2f\16\3"+
		"\2\2\2gh\7U\2\2hi\7C\2\2ij\7X\2\2jk\7G\2\2k\20\3\2\2\2lm\7r\2\2mn\7c\2"+
		"\2no\7t\2\2op\7v\2\2pq\7k\2\2qr\7v\2\2rs\7k\2\2st\7q\2\2tu\7p\2\2uv\7"+
		"D\2\2vw\7{\2\2w\22\3\2\2\2xy\7u\2\2yz\7g\2\2z{\7n\2\2{|\7g\2\2|}\7e\2"+
		"\2}~\7v\2\2~\24\3\2\2\2\177\u0080\7U\2\2\u0080\u0081\7G\2\2\u0081\u0082"+
		"\7N\2\2\u0082\u0083\7G\2\2\u0083\u0084\7E\2\2\u0084\u0085\7V\2\2\u0085"+
		"\26\3\2\2\2\u0086\u0087\7=\2\2\u0087\30\3\2\2\2\u0088\u0089\7k\2\2\u0089"+
		"\u008a\7p\2\2\u008a\u008b\7u\2\2\u008b\u008c\7g\2\2\u008c\u008d\7t\2\2"+
		"\u008d\u008e\7v\2\2\u008e\32\3\2\2\2\u008f\u0090\7K\2\2\u0090\u0091\7"+
		"P\2\2\u0091\u0092\7U\2\2\u0092\u0093\7G\2\2\u0093\u0094\7T\2\2\u0094\u0095"+
		"\7V\2\2\u0095\34\3\2\2\2\u0096\u0097\7e\2\2\u0097\u0098\7t\2\2\u0098\u0099"+
		"\7g\2\2\u0099\u009a\7c\2\2\u009a\u009b\7v\2\2\u009b\u009c\7g\2\2\u009c"+
		"\36\3\2\2\2\u009d\u009e\7E\2\2\u009e\u009f\7T\2\2\u009f\u00a0\7G\2\2\u00a0"+
		"\u00a1\7C\2\2\u00a1\u00a2\7V\2\2\u00a2\u00a3\7G\2\2\u00a3 \3\2\2\2\u00a4"+
		"\u00a5\7u\2\2\u00a5\u00a6\7g\2\2\u00a6\u00a7\7v\2\2\u00a7\"\3\2\2\2\u00a8"+
		"\u00a9\7U\2\2\u00a9\u00aa\7G\2\2\u00aa\u00ab\7V\2\2\u00ab$\3\2\2\2\u00ac"+
		"\u00ad\7e\2\2\u00ad\u00ae\7q\2\2\u00ae\u00af\7p\2\2\u00af\u00b0\7p\2\2"+
		"\u00b0\u00b1\7g\2\2\u00b1\u00b2\7e\2\2\u00b2\u00b3\7v\2\2\u00b3&\3\2\2"+
		"\2\u00b4\u00b5\7E\2\2\u00b5\u00b6\7Q\2\2\u00b6\u00b7\7P\2\2\u00b7\u00b8"+
		"\7P\2\2\u00b8\u00b9\7G\2\2\u00b9\u00ba\7E\2\2\u00ba\u00bb\7V\2\2\u00bb"+
		"(\3\2\2\2\u00bc\u00bd\7y\2\2\u00bd\u00be\7j\2\2\u00be\u00bf\7g\2\2\u00bf"+
		"\u00c0\7t\2\2\u00c0\u00c1\7g\2\2\u00c1*\3\2\2\2\u00c2\u00c3\7q\2\2\u00c3"+
		"\u00c4\7x\2\2\u00c4\u00c5\7g\2\2\u00c5\u00c6\7t\2\2\u00c6\u00c7\7y\2\2"+
		"\u00c7\u00c8\7t\2\2\u00c8\u00c9\7k\2\2\u00c9\u00ca\7v\2\2\u00ca\u00cb"+
		"\7g\2\2\u00cb,\3\2\2\2\u00cc\u00cd\7c\2\2\u00cd\u00ce\7r\2\2\u00ce\u00cf"+
		"\7r\2\2\u00cf\u00d0\7g\2\2\u00d0\u00d1\7p\2\2\u00d1\u00d2\7f\2\2\u00d2"+
		".\3\2\2\2\u00d3\u00d4\7g\2\2\u00d4\u00d5\7t\2\2\u00d5\u00d6\7t\2\2\u00d6"+
		"\u00d7\7q\2\2\u00d7\u00d8\7t\2\2\u00d8\u00d9\7K\2\2\u00d9\u00da\7h\2\2"+
		"\u00da\u00db\7G\2\2\u00db\u00dc\7z\2\2\u00dc\u00dd\7k\2\2\u00dd\u00de"+
		"\7u\2\2\u00de\u00df\7v\2\2\u00df\u00e0\7u\2\2\u00e0\60\3\2\2\2\u00e1\u00e2"+
		"\7k\2\2\u00e2\u00e3\7i\2\2\u00e3\u00e4\7p\2\2\u00e4\u00e5\7q\2\2\u00e5"+
		"\u00e6\7t\2\2\u00e6\u00e7\7g\2\2\u00e7\62\3\2\2\2\u00e8\u00e9\7c\2\2\u00e9"+
		"\u00ea\7p\2\2\u00ea\u00eb\7f\2\2\u00eb\64\3\2\2\2\u00ec\u00ed\7?\2\2\u00ed"+
		"\66\3\2\2\2\u00ee\u00f4\7)\2\2\u00ef\u00f3\n\2\2\2\u00f0\u00f1\7^\2\2"+
		"\u00f1\u00f3\13\2\2\2\u00f2\u00ef\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f3\u00f6"+
		"\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f4\u00f5\3\2\2\2\u00f5\u00f7\3\2\2\2\u00f6"+
		"\u00f4\3\2\2\2\u00f7\u0103\7)\2\2\u00f8\u00fe\7$\2\2\u00f9\u00fd\n\3\2"+
		"\2\u00fa\u00fb\7^\2\2\u00fb\u00fd\13\2\2\2\u00fc\u00f9\3\2\2\2\u00fc\u00fa"+
		"\3\2\2\2\u00fd\u0100\3\2\2\2\u00fe\u00fc\3\2\2\2\u00fe\u00ff\3\2\2\2\u00ff"+
		"\u0101\3\2\2\2\u0100\u00fe\3\2\2\2\u0101\u0103\7$\2\2\u0102\u00ee\3\2"+
		"\2\2\u0102\u00f8\3\2\2\2\u01038\3\2\2\2\u0104\u0108\5? \2\u0105\u0108"+
		"\5=\37\2\u0106\u0108\7a\2\2\u0107\u0104\3\2\2\2\u0107\u0105\3\2\2\2\u0107"+
		"\u0106\3\2\2\2\u0108\u0109\3\2\2\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2"+
		"\2\2\u010a:\3\2\2\2\u010b\u0111\7b\2\2\u010c\u0110\n\4\2\2\u010d\u010e"+
		"\7b\2\2\u010e\u0110\7b\2\2\u010f\u010c\3\2\2\2\u010f\u010d\3\2\2\2\u0110"+
		"\u0113\3\2\2\2\u0111\u010f\3\2\2\2\u0111\u0112\3\2\2\2\u0112\u0114\3\2"+
		"\2\2\u0113\u0111\3\2\2\2\u0114\u0115\7b\2\2\u0115<\3\2\2\2\u0116\u0117"+
		"\t\5\2\2\u0117>\3\2\2\2\u0118\u0119\t\6\2\2\u0119@\3\2\2\2\u011a\u011b"+
		"\7/\2\2\u011b\u011c\7/\2\2\u011c\u0120\3\2\2\2\u011d\u011f\n\7\2\2\u011e"+
		"\u011d\3\2\2\2\u011f\u0122\3\2\2\2\u0120\u011e\3\2\2\2\u0120\u0121\3\2"+
		"\2\2\u0121\u0124\3\2\2\2\u0122\u0120\3\2\2\2\u0123\u0125\7\17\2\2\u0124"+
		"\u0123\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127\3\2\2\2\u0126\u0128\7\f"+
		"\2\2\u0127\u0126\3\2\2\2\u0127\u0128\3\2\2\2\u0128\u0129\3\2\2\2\u0129"+
		"\u012a\b!\2\2\u012aB\3\2\2\2\u012b\u012c\7\61\2\2\u012c\u012d\7,\2\2\u012d"+
		"\u012e\7,\2\2\u012e\u012f\7\61\2\2\u012f\u0130\3\2\2\2\u0130\u0131\b\""+
		"\2\2\u0131D\3\2\2\2\u0132\u0133\7\61\2\2\u0133\u0134\7,\2\2\u0134\u0135"+
		"\3\2\2\2\u0135\u0139\n\b\2\2\u0136\u0138\13\2\2\2\u0137\u0136\3\2\2\2"+
		"\u0138\u013b\3\2\2\2\u0139\u013a\3\2\2\2\u0139\u0137\3\2\2\2\u013a\u013c"+
		"\3\2\2\2\u013b\u0139\3\2\2\2\u013c\u013d\7,\2\2\u013d\u013e\7\61\2\2\u013e"+
		"\u013f\3\2\2\2\u013f\u0140\b#\2\2\u0140F\3\2\2\2\u0141\u0143\t\t\2\2\u0142"+
		"\u0141\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0142\3\2\2\2\u0144\u0145\3\2"+
		"\2\2\u0145\u0146\3\2\2\2\u0146\u0147\b$\2\2\u0147H\3\2\2\2\u0148\u0149"+
		"\13\2\2\2\u0149J\3\2\2\2\21\2\u00f2\u00f4\u00fc\u00fe\u0102\u0107\u0109"+
		"\u010f\u0111\u0120\u0124\u0127\u0139\u0144\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}