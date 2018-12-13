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
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, STRING=30, BLOCK_STRING=31, 
		IDENTIFIER=32, BACKQUOTED_IDENTIFIER=33, SIMPLE_COMMENT=34, BRACKETED_EMPTY_COMMENT=35, 
		BRACKETED_COMMENT=36, WS=37, UNRECOGNIZED=38;
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
		"T__25", "T__26", "T__27", "T__28", "STRING", "BLOCK_STRING", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "DIGIT", "LETTER", "SIMPLE_COMMENT", "BRACKETED_EMPTY_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'load'", "'.'", "'options'", "'where'", "'as'", "'save'", "'partitionBy'", 
		"'partitionby'", "'select'", "';'", "'insert'", "'create'", "'drop'", 
		"'refresh'", "'set'", "'='", "'connect'", "'train'", "'run'", "'predict'", 
		"'register'", "'unregister'", "'include'", "'overwrite'", "'append'", 
		"'errorIfExists'", "'ignore'", "'and'", "','", null, null, null, null, 
		null, "'/**/'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, "STRING", "BLOCK_STRING", "IDENTIFIER", 
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2(\u017f\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\3\2\3\2\3\2\3"+
		"\2\3\2\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3"+
		"\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r"+
		"\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\17\3\20\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3"+
		"\22\3\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\25\3\25\3"+
		"\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3\26\3"+
		"\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\30\3\30\3"+
		"\30\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3"+
		"\31\3\31\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3"+
		"\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\34\3\34\3"+
		"\34\3\34\3\35\3\35\3\35\3\35\3\36\3\36\3\37\3\37\3\37\3\37\7\37\u0119"+
		"\n\37\f\37\16\37\u011c\13\37\3\37\3\37\3\37\3\37\3\37\7\37\u0123\n\37"+
		"\f\37\16\37\u0126\13\37\3\37\5\37\u0129\n\37\3 \3 \3 \3 \3 \3 \7 \u0131"+
		"\n \f \16 \u0134\13 \3 \3 \3 \3 \3!\3!\3!\6!\u013d\n!\r!\16!\u013e\3\""+
		"\3\"\3\"\3\"\7\"\u0145\n\"\f\"\16\"\u0148\13\"\3\"\3\"\3#\3#\3$\3$\3%"+
		"\3%\3%\3%\7%\u0154\n%\f%\16%\u0157\13%\3%\5%\u015a\n%\3%\5%\u015d\n%\3"+
		"%\3%\3&\3&\3&\3&\3&\3&\3&\3\'\3\'\3\'\3\'\3\'\7\'\u016d\n\'\f\'\16\'\u0170"+
		"\13\'\3\'\3\'\3\'\3\'\3\'\3(\6(\u0178\n(\r(\16(\u0179\3(\3(\3)\3)\4\u0132"+
		"\u016e\2*\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36;\37= ?!A\"C#E\2G\2I$K%M&O\'Q(\3\2\n\4\2))^^\4\2$$^^\3\2--\3\2"+
		"bb\3\2\62;\4\2C\\c|\4\2\f\f\17\17\5\2\13\f\17\17\"\"\2\u018c\2\3\3\2\2"+
		"\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3"+
		"\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2"+
		"\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2"+
		"\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2"+
		"\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3"+
		"\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2"+
		"\2\2O\3\2\2\2\2Q\3\2\2\2\3S\3\2\2\2\5X\3\2\2\2\7Z\3\2\2\2\tb\3\2\2\2\13"+
		"h\3\2\2\2\rk\3\2\2\2\17p\3\2\2\2\21|\3\2\2\2\23\u0088\3\2\2\2\25\u008f"+
		"\3\2\2\2\27\u0091\3\2\2\2\31\u0098\3\2\2\2\33\u009f\3\2\2\2\35\u00a4\3"+
		"\2\2\2\37\u00ac\3\2\2\2!\u00b0\3\2\2\2#\u00b2\3\2\2\2%\u00ba\3\2\2\2\'"+
		"\u00c0\3\2\2\2)\u00c4\3\2\2\2+\u00cc\3\2\2\2-\u00d5\3\2\2\2/\u00e0\3\2"+
		"\2\2\61\u00e8\3\2\2\2\63\u00f2\3\2\2\2\65\u00f9\3\2\2\2\67\u0107\3\2\2"+
		"\29\u010e\3\2\2\2;\u0112\3\2\2\2=\u0128\3\2\2\2?\u012a\3\2\2\2A\u013c"+
		"\3\2\2\2C\u0140\3\2\2\2E\u014b\3\2\2\2G\u014d\3\2\2\2I\u014f\3\2\2\2K"+
		"\u0160\3\2\2\2M\u0167\3\2\2\2O\u0177\3\2\2\2Q\u017d\3\2\2\2ST\7n\2\2T"+
		"U\7q\2\2UV\7c\2\2VW\7f\2\2W\4\3\2\2\2XY\7\60\2\2Y\6\3\2\2\2Z[\7q\2\2["+
		"\\\7r\2\2\\]\7v\2\2]^\7k\2\2^_\7q\2\2_`\7p\2\2`a\7u\2\2a\b\3\2\2\2bc\7"+
		"y\2\2cd\7j\2\2de\7g\2\2ef\7t\2\2fg\7g\2\2g\n\3\2\2\2hi\7c\2\2ij\7u\2\2"+
		"j\f\3\2\2\2kl\7u\2\2lm\7c\2\2mn\7x\2\2no\7g\2\2o\16\3\2\2\2pq\7r\2\2q"+
		"r\7c\2\2rs\7t\2\2st\7v\2\2tu\7k\2\2uv\7v\2\2vw\7k\2\2wx\7q\2\2xy\7p\2"+
		"\2yz\7D\2\2z{\7{\2\2{\20\3\2\2\2|}\7r\2\2}~\7c\2\2~\177\7t\2\2\177\u0080"+
		"\7v\2\2\u0080\u0081\7k\2\2\u0081\u0082\7v\2\2\u0082\u0083\7k\2\2\u0083"+
		"\u0084\7q\2\2\u0084\u0085\7p\2\2\u0085\u0086\7d\2\2\u0086\u0087\7{\2\2"+
		"\u0087\22\3\2\2\2\u0088\u0089\7u\2\2\u0089\u008a\7g\2\2\u008a\u008b\7"+
		"n\2\2\u008b\u008c\7g\2\2\u008c\u008d\7e\2\2\u008d\u008e\7v\2\2\u008e\24"+
		"\3\2\2\2\u008f\u0090\7=\2\2\u0090\26\3\2\2\2\u0091\u0092\7k\2\2\u0092"+
		"\u0093\7p\2\2\u0093\u0094\7u\2\2\u0094\u0095\7g\2\2\u0095\u0096\7t\2\2"+
		"\u0096\u0097\7v\2\2\u0097\30\3\2\2\2\u0098\u0099\7e\2\2\u0099\u009a\7"+
		"t\2\2\u009a\u009b\7g\2\2\u009b\u009c\7c\2\2\u009c\u009d\7v\2\2\u009d\u009e"+
		"\7g\2\2\u009e\32\3\2\2\2\u009f\u00a0\7f\2\2\u00a0\u00a1\7t\2\2\u00a1\u00a2"+
		"\7q\2\2\u00a2\u00a3\7r\2\2\u00a3\34\3\2\2\2\u00a4\u00a5\7t\2\2\u00a5\u00a6"+
		"\7g\2\2\u00a6\u00a7\7h\2\2\u00a7\u00a8\7t\2\2\u00a8\u00a9\7g\2\2\u00a9"+
		"\u00aa\7u\2\2\u00aa\u00ab\7j\2\2\u00ab\36\3\2\2\2\u00ac\u00ad\7u\2\2\u00ad"+
		"\u00ae\7g\2\2\u00ae\u00af\7v\2\2\u00af \3\2\2\2\u00b0\u00b1\7?\2\2\u00b1"+
		"\"\3\2\2\2\u00b2\u00b3\7e\2\2\u00b3\u00b4\7q\2\2\u00b4\u00b5\7p\2\2\u00b5"+
		"\u00b6\7p\2\2\u00b6\u00b7\7g\2\2\u00b7\u00b8\7e\2\2\u00b8\u00b9\7v\2\2"+
		"\u00b9$\3\2\2\2\u00ba\u00bb\7v\2\2\u00bb\u00bc\7t\2\2\u00bc\u00bd\7c\2"+
		"\2\u00bd\u00be\7k\2\2\u00be\u00bf\7p\2\2\u00bf&\3\2\2\2\u00c0\u00c1\7"+
		"t\2\2\u00c1\u00c2\7w\2\2\u00c2\u00c3\7p\2\2\u00c3(\3\2\2\2\u00c4\u00c5"+
		"\7r\2\2\u00c5\u00c6\7t\2\2\u00c6\u00c7\7g\2\2\u00c7\u00c8\7f\2\2\u00c8"+
		"\u00c9\7k\2\2\u00c9\u00ca\7e\2\2\u00ca\u00cb\7v\2\2\u00cb*\3\2\2\2\u00cc"+
		"\u00cd\7t\2\2\u00cd\u00ce\7g\2\2\u00ce\u00cf\7i\2\2\u00cf\u00d0\7k\2\2"+
		"\u00d0\u00d1\7u\2\2\u00d1\u00d2\7v\2\2\u00d2\u00d3\7g\2\2\u00d3\u00d4"+
		"\7t\2\2\u00d4,\3\2\2\2\u00d5\u00d6\7w\2\2\u00d6\u00d7\7p\2\2\u00d7\u00d8"+
		"\7t\2\2\u00d8\u00d9\7g\2\2\u00d9\u00da\7i\2\2\u00da\u00db\7k\2\2\u00db"+
		"\u00dc\7u\2\2\u00dc\u00dd\7v\2\2\u00dd\u00de\7g\2\2\u00de\u00df\7t\2\2"+
		"\u00df.\3\2\2\2\u00e0\u00e1\7k\2\2\u00e1\u00e2\7p\2\2\u00e2\u00e3\7e\2"+
		"\2\u00e3\u00e4\7n\2\2\u00e4\u00e5\7w\2\2\u00e5\u00e6\7f\2\2\u00e6\u00e7"+
		"\7g\2\2\u00e7\60\3\2\2\2\u00e8\u00e9\7q\2\2\u00e9\u00ea\7x\2\2\u00ea\u00eb"+
		"\7g\2\2\u00eb\u00ec\7t\2\2\u00ec\u00ed\7y\2\2\u00ed\u00ee\7t\2\2\u00ee"+
		"\u00ef\7k\2\2\u00ef\u00f0\7v\2\2\u00f0\u00f1\7g\2\2\u00f1\62\3\2\2\2\u00f2"+
		"\u00f3\7c\2\2\u00f3\u00f4\7r\2\2\u00f4\u00f5\7r\2\2\u00f5\u00f6\7g\2\2"+
		"\u00f6\u00f7\7p\2\2\u00f7\u00f8\7f\2\2\u00f8\64\3\2\2\2\u00f9\u00fa\7"+
		"g\2\2\u00fa\u00fb\7t\2\2\u00fb\u00fc\7t\2\2\u00fc\u00fd\7q\2\2\u00fd\u00fe"+
		"\7t\2\2\u00fe\u00ff\7K\2\2\u00ff\u0100\7h\2\2\u0100\u0101\7G\2\2\u0101"+
		"\u0102\7z\2\2\u0102\u0103\7k\2\2\u0103\u0104\7u\2\2\u0104\u0105\7v\2\2"+
		"\u0105\u0106\7u\2\2\u0106\66\3\2\2\2\u0107\u0108\7k\2\2\u0108\u0109\7"+
		"i\2\2\u0109\u010a\7p\2\2\u010a\u010b\7q\2\2\u010b\u010c\7t\2\2\u010c\u010d"+
		"\7g\2\2\u010d8\3\2\2\2\u010e\u010f\7c\2\2\u010f\u0110\7p\2\2\u0110\u0111"+
		"\7f\2\2\u0111:\3\2\2\2\u0112\u0113\7.\2\2\u0113<\3\2\2\2\u0114\u011a\7"+
		")\2\2\u0115\u0119\n\2\2\2\u0116\u0117\7^\2\2\u0117\u0119\13\2\2\2\u0118"+
		"\u0115\3\2\2\2\u0118\u0116\3\2\2\2\u0119\u011c\3\2\2\2\u011a\u0118\3\2"+
		"\2\2\u011a\u011b\3\2\2\2\u011b\u011d\3\2\2\2\u011c\u011a\3\2\2\2\u011d"+
		"\u0129\7)\2\2\u011e\u0124\7$\2\2\u011f\u0123\n\3\2\2\u0120\u0121\7^\2"+
		"\2\u0121\u0123\13\2\2\2\u0122\u011f\3\2\2\2\u0122\u0120\3\2\2\2\u0123"+
		"\u0126\3\2\2\2\u0124\u0122\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127\3\2"+
		"\2\2\u0126\u0124\3\2\2\2\u0127\u0129\7$\2\2\u0128\u0114\3\2\2\2\u0128"+
		"\u011e\3\2\2\2\u0129>\3\2\2\2\u012a\u012b\7)\2\2\u012b\u012c\7)\2\2\u012c"+
		"\u012d\7)\2\2\u012d\u012e\3\2\2\2\u012e\u0132\n\4\2\2\u012f\u0131\13\2"+
		"\2\2\u0130\u012f\3\2\2\2\u0131\u0134\3\2\2\2\u0132\u0133\3\2\2\2\u0132"+
		"\u0130\3\2\2\2\u0133\u0135\3\2\2\2\u0134\u0132\3\2\2\2\u0135\u0136\7)"+
		"\2\2\u0136\u0137\7)\2\2\u0137\u0138\7)\2\2\u0138@\3\2\2\2\u0139\u013d"+
		"\5G$\2\u013a\u013d\5E#\2\u013b\u013d\7a\2\2\u013c\u0139\3\2\2\2\u013c"+
		"\u013a\3\2\2\2\u013c\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013c\3\2"+
		"\2\2\u013e\u013f\3\2\2\2\u013fB\3\2\2\2\u0140\u0146\7b\2\2\u0141\u0145"+
		"\n\5\2\2\u0142\u0143\7b\2\2\u0143\u0145\7b\2\2\u0144\u0141\3\2\2\2\u0144"+
		"\u0142\3\2\2\2\u0145\u0148\3\2\2\2\u0146\u0144\3\2\2\2\u0146\u0147\3\2"+
		"\2\2\u0147\u0149\3\2\2\2\u0148\u0146\3\2\2\2\u0149\u014a\7b\2\2\u014a"+
		"D\3\2\2\2\u014b\u014c\t\6\2\2\u014cF\3\2\2\2\u014d\u014e\t\7\2\2\u014e"+
		"H\3\2\2\2\u014f\u0150\7/\2\2\u0150\u0151\7/\2\2\u0151\u0155\3\2\2\2\u0152"+
		"\u0154\n\b\2\2\u0153\u0152\3\2\2\2\u0154\u0157\3\2\2\2\u0155\u0153\3\2"+
		"\2\2\u0155\u0156\3\2\2\2\u0156\u0159\3\2\2\2\u0157\u0155\3\2\2\2\u0158"+
		"\u015a\7\17\2\2\u0159\u0158\3\2\2\2\u0159\u015a\3\2\2\2\u015a\u015c\3"+
		"\2\2\2\u015b\u015d\7\f\2\2\u015c\u015b\3\2\2\2\u015c\u015d\3\2\2\2\u015d"+
		"\u015e\3\2\2\2\u015e\u015f\b%\2\2\u015fJ\3\2\2\2\u0160\u0161\7\61\2\2"+
		"\u0161\u0162\7,\2\2\u0162\u0163\7,\2\2\u0163\u0164\7\61\2\2\u0164\u0165"+
		"\3\2\2\2\u0165\u0166\b&\2\2\u0166L\3\2\2\2\u0167\u0168\7\61\2\2\u0168"+
		"\u0169\7,\2\2\u0169\u016a\3\2\2\2\u016a\u016e\n\4\2\2\u016b\u016d\13\2"+
		"\2\2\u016c\u016b\3\2\2\2\u016d\u0170\3\2\2\2\u016e\u016f\3\2\2\2\u016e"+
		"\u016c\3\2\2\2\u016f\u0171\3\2\2\2\u0170\u016e\3\2\2\2\u0171\u0172\7,"+
		"\2\2\u0172\u0173\7\61\2\2\u0173\u0174\3\2\2\2\u0174\u0175\b\'\2\2\u0175"+
		"N\3\2\2\2\u0176\u0178\t\t\2\2\u0177\u0176\3\2\2\2\u0178\u0179\3\2\2\2"+
		"\u0179\u0177\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u017c"+
		"\b(\2\2\u017cP\3\2\2\2\u017d\u017e\13\2\2\2\u017eR\3\2\2\2\22\2\u0118"+
		"\u011a\u0122\u0124\u0128\u0132\u013c\u013e\u0144\u0146\u0155\u0159\u015c"+
		"\u016e\u0179\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}