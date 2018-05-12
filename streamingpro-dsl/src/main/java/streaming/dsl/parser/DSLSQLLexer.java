// Generated from /Users/allwefantasy/CSDNWorkSpace/streamingpro/streamingpro-dsl/src/main/resources/DSLSQL.g4 by ANTLR 4.7.1

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
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31, 
		T__31=32, T__32=33, T__33=34, STRING=35, IDENTIFIER=36, BACKQUOTED_IDENTIFIER=37, 
		SIMPLE_COMMENT=38, BRACKETED_EMPTY_COMMENT=39, BRACKETED_COMMENT=40, WS=41, 
		UNRECOGNIZED=42;
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
		"T__25", "T__26", "T__27", "T__28", "T__29", "T__30", "T__31", "T__32", 
		"T__33", "STRING", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", 
		"SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", "BRACKETED_COMMENT", "WS", 
		"UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'LOAD'", "'.'", "'options'", "'as'", "'save'", "'SAVE'", 
		"'partitionBy'", "'select'", "'SELECT'", "';'", "'insert'", "'INSERT'", 
		"'create'", "'CREATE'", "'drop'", "'DROP'", "'refresh'", "'REFRESH'", 
		"'set'", "'SET'", "'='", "'connect'", "'CONNECT'", "'where'", "'train'", 
		"'TRAIN'", "'register'", "'REGISTER'", "'overwrite'", "'append'", "'errorIfExists'", 
		"'ignore'", "'and'", null, null, null, null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, "STRING", 
		"IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2,\u0192\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\3\2\3\2\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\4\3\4\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3"+
		"\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3\r\3"+
		"\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21"+
		"\3\22\3\22\3\22\3\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\24"+
		"\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\25\3\26\3\26\3\26"+
		"\3\26\3\27\3\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31"+
		"\3\31\3\31\3\31\3\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33"+
		"\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3\34\3\35\3\35\3\35\3\35\3\35"+
		"\3\35\3\35\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\36\3\37"+
		"\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3 \3 \3 \3 \3 \3 \3 \3!"+
		"\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3"+
		"#\3#\3#\3#\3$\3$\3$\3$\7$\u013b\n$\f$\16$\u013e\13$\3$\3$\3$\3$\3$\7$"+
		"\u0145\n$\f$\16$\u0148\13$\3$\5$\u014b\n$\3%\3%\3%\6%\u0150\n%\r%\16%"+
		"\u0151\3&\3&\3&\3&\7&\u0158\n&\f&\16&\u015b\13&\3&\3&\3\'\3\'\3(\3(\3"+
		")\3)\3)\3)\7)\u0167\n)\f)\16)\u016a\13)\3)\5)\u016d\n)\3)\5)\u0170\n)"+
		"\3)\3)\3*\3*\3*\3*\3*\3*\3*\3+\3+\3+\3+\3+\7+\u0180\n+\f+\16+\u0183\13"+
		"+\3+\3+\3+\3+\3+\3,\6,\u018b\n,\r,\16,\u018c\3,\3,\3-\3-\3\u0181\2.\3"+
		"\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37"+
		"\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37="+
		" ?!A\"C#E$G%I&K\'M\2O\2Q(S)U*W+Y,\3\2\n\4\2))^^\4\2$$^^\3\2bb\3\2\62;"+
		"\4\2C\\c|\4\2\f\f\17\17\3\2--\5\2\13\f\17\17\"\"\2\u019e\2\3\3\2\2\2\2"+
		"\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2"+
		"\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2"+
		"\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2"+
		"\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2"+
		"\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2"+
		"\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2"+
		"K\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\3[\3"+
		"\2\2\2\5`\3\2\2\2\7e\3\2\2\2\tg\3\2\2\2\13o\3\2\2\2\rr\3\2\2\2\17w\3\2"+
		"\2\2\21|\3\2\2\2\23\u0088\3\2\2\2\25\u008f\3\2\2\2\27\u0096\3\2\2\2\31"+
		"\u0098\3\2\2\2\33\u009f\3\2\2\2\35\u00a6\3\2\2\2\37\u00ad\3\2\2\2!\u00b4"+
		"\3\2\2\2#\u00b9\3\2\2\2%\u00be\3\2\2\2\'\u00c6\3\2\2\2)\u00ce\3\2\2\2"+
		"+\u00d2\3\2\2\2-\u00d6\3\2\2\2/\u00d8\3\2\2\2\61\u00e0\3\2\2\2\63\u00e8"+
		"\3\2\2\2\65\u00ee\3\2\2\2\67\u00f4\3\2\2\29\u00fa\3\2\2\2;\u0103\3\2\2"+
		"\2=\u010c\3\2\2\2?\u0116\3\2\2\2A\u011d\3\2\2\2C\u012b\3\2\2\2E\u0132"+
		"\3\2\2\2G\u014a\3\2\2\2I\u014f\3\2\2\2K\u0153\3\2\2\2M\u015e\3\2\2\2O"+
		"\u0160\3\2\2\2Q\u0162\3\2\2\2S\u0173\3\2\2\2U\u017a\3\2\2\2W\u018a\3\2"+
		"\2\2Y\u0190\3\2\2\2[\\\7n\2\2\\]\7q\2\2]^\7c\2\2^_\7f\2\2_\4\3\2\2\2`"+
		"a\7N\2\2ab\7Q\2\2bc\7C\2\2cd\7F\2\2d\6\3\2\2\2ef\7\60\2\2f\b\3\2\2\2g"+
		"h\7q\2\2hi\7r\2\2ij\7v\2\2jk\7k\2\2kl\7q\2\2lm\7p\2\2mn\7u\2\2n\n\3\2"+
		"\2\2op\7c\2\2pq\7u\2\2q\f\3\2\2\2rs\7u\2\2st\7c\2\2tu\7x\2\2uv\7g\2\2"+
		"v\16\3\2\2\2wx\7U\2\2xy\7C\2\2yz\7X\2\2z{\7G\2\2{\20\3\2\2\2|}\7r\2\2"+
		"}~\7c\2\2~\177\7t\2\2\177\u0080\7v\2\2\u0080\u0081\7k\2\2\u0081\u0082"+
		"\7v\2\2\u0082\u0083\7k\2\2\u0083\u0084\7q\2\2\u0084\u0085\7p\2\2\u0085"+
		"\u0086\7D\2\2\u0086\u0087\7{\2\2\u0087\22\3\2\2\2\u0088\u0089\7u\2\2\u0089"+
		"\u008a\7g\2\2\u008a\u008b\7n\2\2\u008b\u008c\7g\2\2\u008c\u008d\7e\2\2"+
		"\u008d\u008e\7v\2\2\u008e\24\3\2\2\2\u008f\u0090\7U\2\2\u0090\u0091\7"+
		"G\2\2\u0091\u0092\7N\2\2\u0092\u0093\7G\2\2\u0093\u0094\7E\2\2\u0094\u0095"+
		"\7V\2\2\u0095\26\3\2\2\2\u0096\u0097\7=\2\2\u0097\30\3\2\2\2\u0098\u0099"+
		"\7k\2\2\u0099\u009a\7p\2\2\u009a\u009b\7u\2\2\u009b\u009c\7g\2\2\u009c"+
		"\u009d\7t\2\2\u009d\u009e\7v\2\2\u009e\32\3\2\2\2\u009f\u00a0\7K\2\2\u00a0"+
		"\u00a1\7P\2\2\u00a1\u00a2\7U\2\2\u00a2\u00a3\7G\2\2\u00a3\u00a4\7T\2\2"+
		"\u00a4\u00a5\7V\2\2\u00a5\34\3\2\2\2\u00a6\u00a7\7e\2\2\u00a7\u00a8\7"+
		"t\2\2\u00a8\u00a9\7g\2\2\u00a9\u00aa\7c\2\2\u00aa\u00ab\7v\2\2\u00ab\u00ac"+
		"\7g\2\2\u00ac\36\3\2\2\2\u00ad\u00ae\7E\2\2\u00ae\u00af\7T\2\2\u00af\u00b0"+
		"\7G\2\2\u00b0\u00b1\7C\2\2\u00b1\u00b2\7V\2\2\u00b2\u00b3\7G\2\2\u00b3"+
		" \3\2\2\2\u00b4\u00b5\7f\2\2\u00b5\u00b6\7t\2\2\u00b6\u00b7\7q\2\2\u00b7"+
		"\u00b8\7r\2\2\u00b8\"\3\2\2\2\u00b9\u00ba\7F\2\2\u00ba\u00bb\7T\2\2\u00bb"+
		"\u00bc\7Q\2\2\u00bc\u00bd\7R\2\2\u00bd$\3\2\2\2\u00be\u00bf\7t\2\2\u00bf"+
		"\u00c0\7g\2\2\u00c0\u00c1\7h\2\2\u00c1\u00c2\7t\2\2\u00c2\u00c3\7g\2\2"+
		"\u00c3\u00c4\7u\2\2\u00c4\u00c5\7j\2\2\u00c5&\3\2\2\2\u00c6\u00c7\7T\2"+
		"\2\u00c7\u00c8\7G\2\2\u00c8\u00c9\7H\2\2\u00c9\u00ca\7T\2\2\u00ca\u00cb"+
		"\7G\2\2\u00cb\u00cc\7U\2\2\u00cc\u00cd\7J\2\2\u00cd(\3\2\2\2\u00ce\u00cf"+
		"\7u\2\2\u00cf\u00d0\7g\2\2\u00d0\u00d1\7v\2\2\u00d1*\3\2\2\2\u00d2\u00d3"+
		"\7U\2\2\u00d3\u00d4\7G\2\2\u00d4\u00d5\7V\2\2\u00d5,\3\2\2\2\u00d6\u00d7"+
		"\7?\2\2\u00d7.\3\2\2\2\u00d8\u00d9\7e\2\2\u00d9\u00da\7q\2\2\u00da\u00db"+
		"\7p\2\2\u00db\u00dc\7p\2\2\u00dc\u00dd\7g\2\2\u00dd\u00de\7e\2\2\u00de"+
		"\u00df\7v\2\2\u00df\60\3\2\2\2\u00e0\u00e1\7E\2\2\u00e1\u00e2\7Q\2\2\u00e2"+
		"\u00e3\7P\2\2\u00e3\u00e4\7P\2\2\u00e4\u00e5\7G\2\2\u00e5\u00e6\7E\2\2"+
		"\u00e6\u00e7\7V\2\2\u00e7\62\3\2\2\2\u00e8\u00e9\7y\2\2\u00e9\u00ea\7"+
		"j\2\2\u00ea\u00eb\7g\2\2\u00eb\u00ec\7t\2\2\u00ec\u00ed\7g\2\2\u00ed\64"+
		"\3\2\2\2\u00ee\u00ef\7v\2\2\u00ef\u00f0\7t\2\2\u00f0\u00f1\7c\2\2\u00f1"+
		"\u00f2\7k\2\2\u00f2\u00f3\7p\2\2\u00f3\66\3\2\2\2\u00f4\u00f5\7V\2\2\u00f5"+
		"\u00f6\7T\2\2\u00f6\u00f7\7C\2\2\u00f7\u00f8\7K\2\2\u00f8\u00f9\7P\2\2"+
		"\u00f98\3\2\2\2\u00fa\u00fb\7t\2\2\u00fb\u00fc\7g\2\2\u00fc\u00fd\7i\2"+
		"\2\u00fd\u00fe\7k\2\2\u00fe\u00ff\7u\2\2\u00ff\u0100\7v\2\2\u0100\u0101"+
		"\7g\2\2\u0101\u0102\7t\2\2\u0102:\3\2\2\2\u0103\u0104\7T\2\2\u0104\u0105"+
		"\7G\2\2\u0105\u0106\7I\2\2\u0106\u0107\7K\2\2\u0107\u0108\7U\2\2\u0108"+
		"\u0109\7V\2\2\u0109\u010a\7G\2\2\u010a\u010b\7T\2\2\u010b<\3\2\2\2\u010c"+
		"\u010d\7q\2\2\u010d\u010e\7x\2\2\u010e\u010f\7g\2\2\u010f\u0110\7t\2\2"+
		"\u0110\u0111\7y\2\2\u0111\u0112\7t\2\2\u0112\u0113\7k\2\2\u0113\u0114"+
		"\7v\2\2\u0114\u0115\7g\2\2\u0115>\3\2\2\2\u0116\u0117\7c\2\2\u0117\u0118"+
		"\7r\2\2\u0118\u0119\7r\2\2\u0119\u011a\7g\2\2\u011a\u011b\7p\2\2\u011b"+
		"\u011c\7f\2\2\u011c@\3\2\2\2\u011d\u011e\7g\2\2\u011e\u011f\7t\2\2\u011f"+
		"\u0120\7t\2\2\u0120\u0121\7q\2\2\u0121\u0122\7t\2\2\u0122\u0123\7K\2\2"+
		"\u0123\u0124\7h\2\2\u0124\u0125\7G\2\2\u0125\u0126\7z\2\2\u0126\u0127"+
		"\7k\2\2\u0127\u0128\7u\2\2\u0128\u0129\7v\2\2\u0129\u012a\7u\2\2\u012a"+
		"B\3\2\2\2\u012b\u012c\7k\2\2\u012c\u012d\7i\2\2\u012d\u012e\7p\2\2\u012e"+
		"\u012f\7q\2\2\u012f\u0130\7t\2\2\u0130\u0131\7g\2\2\u0131D\3\2\2\2\u0132"+
		"\u0133\7c\2\2\u0133\u0134\7p\2\2\u0134\u0135\7f\2\2\u0135F\3\2\2\2\u0136"+
		"\u013c\7)\2\2\u0137\u013b\n\2\2\2\u0138\u0139\7^\2\2\u0139\u013b\13\2"+
		"\2\2\u013a\u0137\3\2\2\2\u013a\u0138\3\2\2\2\u013b\u013e\3\2\2\2\u013c"+
		"\u013a\3\2\2\2\u013c\u013d\3\2\2\2\u013d\u013f\3\2\2\2\u013e\u013c\3\2"+
		"\2\2\u013f\u014b\7)\2\2\u0140\u0146\7$\2\2\u0141\u0145\n\3\2\2\u0142\u0143"+
		"\7^\2\2\u0143\u0145\13\2\2\2\u0144\u0141\3\2\2\2\u0144\u0142\3\2\2\2\u0145"+
		"\u0148\3\2\2\2\u0146\u0144\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0149\3\2"+
		"\2\2\u0148\u0146\3\2\2\2\u0149\u014b\7$\2\2\u014a\u0136\3\2\2\2\u014a"+
		"\u0140\3\2\2\2\u014bH\3\2\2\2\u014c\u0150\5O(\2\u014d\u0150\5M\'\2\u014e"+
		"\u0150\7a\2\2\u014f\u014c\3\2\2\2\u014f\u014d\3\2\2\2\u014f\u014e\3\2"+
		"\2\2\u0150\u0151\3\2\2\2\u0151\u014f\3\2\2\2\u0151\u0152\3\2\2\2\u0152"+
		"J\3\2\2\2\u0153\u0159\7b\2\2\u0154\u0158\n\4\2\2\u0155\u0156\7b\2\2\u0156"+
		"\u0158\7b\2\2\u0157\u0154\3\2\2\2\u0157\u0155\3\2\2\2\u0158\u015b\3\2"+
		"\2\2\u0159\u0157\3\2\2\2\u0159\u015a\3\2\2\2\u015a\u015c\3\2\2\2\u015b"+
		"\u0159\3\2\2\2\u015c\u015d\7b\2\2\u015dL\3\2\2\2\u015e\u015f\t\5\2\2\u015f"+
		"N\3\2\2\2\u0160\u0161\t\6\2\2\u0161P\3\2\2\2\u0162\u0163\7/\2\2\u0163"+
		"\u0164\7/\2\2\u0164\u0168\3\2\2\2\u0165\u0167\n\7\2\2\u0166\u0165\3\2"+
		"\2\2\u0167\u016a\3\2\2\2\u0168\u0166\3\2\2\2\u0168\u0169\3\2\2\2\u0169"+
		"\u016c\3\2\2\2\u016a\u0168\3\2\2\2\u016b\u016d\7\17\2\2\u016c\u016b\3"+
		"\2\2\2\u016c\u016d\3\2\2\2\u016d\u016f\3\2\2\2\u016e\u0170\7\f\2\2\u016f"+
		"\u016e\3\2\2\2\u016f\u0170\3\2\2\2\u0170\u0171\3\2\2\2\u0171\u0172\b)"+
		"\2\2\u0172R\3\2\2\2\u0173\u0174\7\61\2\2\u0174\u0175\7,\2\2\u0175\u0176"+
		"\7,\2\2\u0176\u0177\7\61\2\2\u0177\u0178\3\2\2\2\u0178\u0179\b*\2\2\u0179"+
		"T\3\2\2\2\u017a\u017b\7\61\2\2\u017b\u017c\7,\2\2\u017c\u017d\3\2\2\2"+
		"\u017d\u0181\n\b\2\2\u017e\u0180\13\2\2\2\u017f\u017e\3\2\2\2\u0180\u0183"+
		"\3\2\2\2\u0181\u0182\3\2\2\2\u0181\u017f\3\2\2\2\u0182\u0184\3\2\2\2\u0183"+
		"\u0181\3\2\2\2\u0184\u0185\7,\2\2\u0185\u0186\7\61\2\2\u0186\u0187\3\2"+
		"\2\2\u0187\u0188\b+\2\2\u0188V\3\2\2\2\u0189\u018b\t\t\2\2\u018a\u0189"+
		"\3\2\2\2\u018b\u018c\3\2\2\2\u018c\u018a\3\2\2\2\u018c\u018d\3\2\2\2\u018d"+
		"\u018e\3\2\2\2\u018e\u018f\b,\2\2\u018fX\3\2\2\2\u0190\u0191\13\2\2\2"+
		"\u0191Z\3\2\2\2\21\2\u013a\u013c\u0144\u0146\u014a\u014f\u0151\u0157\u0159"+
		"\u0168\u016c\u016f\u0181\u018c\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}