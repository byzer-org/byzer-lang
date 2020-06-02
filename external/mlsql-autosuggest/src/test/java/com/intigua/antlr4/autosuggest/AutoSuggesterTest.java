package com.intigua.antlr4.autosuggest;

import org.antlr.runtime.RecognitionException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.LexerGrammar;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.intigua.antlr4.autosuggest.CasePreference.*;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class AutoSuggesterTest {

    private final static String DEFAULT_LOG_LEVEL = "WARN";
    private LexerAndParserFactory lexerAndParserFactory;
    private Collection<String> suggestedCompletions;
    private CasePreference casePreference = null;

    @BeforeClass
    public static void initLogging() {
    }

    @Test
    public void suggest_withEmpty_shouldSuggestFirstToken() {
        givenGrammar("r: 'AB' 'CD'").whenInput("").thenExpect("AB");
    }

    @Test
    public void suggest_withMultiCharInputAllAtoms_shouldCompleteIt() {
        givenGrammar("r: 'ABC'").whenInput("AB").thenExpect("C");
    }

    @Test
    public void suggest_withSingleTokenComingUp_shouldSuggestSingleToken() {
        givenGrammar("r: 'AB' 'CD'").whenInput("AB").thenExpect("CD");
    }

    @Test
    public void suggest_withTokenAndAHalf_shouldCompleteTheToken() {
        givenGrammar("r: 'AB' 'CD'").whenInput("ABC").thenExpect("D");
    }

    @Test
    public void suggest_withHalfToken_shouldCompleteTheToken() {
        givenGrammar("r: 'AB' 'CD'").whenInput("A").thenExpect("B");
    }

    @Test
    public void suggest_withCompleteExpression_shouldNotSuggestAnything() {
        givenGrammar("r: 'AB' 'CD'").whenInput("ABCD").thenExpect();
    }

    @Test
    public void suggest_withWrongCompletion_shouldNotSuggest() {
        givenGrammar("r: 'AB' 'CD'").whenInput("ABD").thenExpect();
    }

    @Test
    public void suggest_withParens_shouldSuggest() {
        givenGrammar("r: ('AB') ('CD')").whenInput("AB").thenExpect("CD");
    }

    @Test
    public void suggest_withOptional_shouldSuggest() {
        givenGrammar("r: 'A'? 'B'").whenInput("").thenExpect("A", "B");
    }

    @Test
    public void suggest_withAlternativeTokens_shouldSuggest() {
        givenGrammar("r: 'A' | 'B'").whenInput("").thenExpect("A", "B");
    }

    @Test
    public void suggest_withOptionalParserCompletion_shouldSuggest() {
        givenGrammar("r: 'A' 'B'?").whenInput("A").thenExpect("B");
    }

    @Test
    public void suggest_withAlternativeParserRules_shouldSuggest() {
        givenGrammar("r: a | b", "a: 'A'", "b: 'B'").whenInput("").thenExpect("A", "B");
    }

    @Test
    public void suggest_withTokenRange_shouldSuggestEntireRange() {
        givenGrammar("r: A", "A: [A-E]").whenInput("").thenExpect("A", "B", "C", "D", "E");
    }

    @Test
    public void suggest_withTokenRangeMatchingPartial_shouldSuggestJustTheMatch() {
        givenGrammar("r: A", "A: [A-E] 'X'").whenInput("C").thenExpect("X");
    }

    @Test
    public void suggest_withSingleAtomTokenAndSetCompletion_shouldSuggest() {
        givenGrammar("r: A", "A: 'AB' [C-E] 'X'").whenInput("AB").thenExpect("CX", "DX", "EX");
    }

    @Test
    public void suggest_withAtomAndSetTokenAndSetCompletion_shouldSuggest() {
        givenGrammar("r: A", "A: 'A' [B-C] [D-E]").whenInput("AB").thenExpect("D", "E");
    }

    @Test
    public void suggest_withTwoSetsAndSetCompletion_shouldSuggest() {
        givenGrammar("r: A", "A: [A-B] [C-D] [E-F]").whenInput("AD").thenExpect("E", "F");
    }

    @Test
    public void suggest_withTokenRangeInFragment_shouldNotSuggest() {
        givenGrammar("r: A", "fragment A: [A-Z]").whenInput("").thenExpect();
    }

    @Test
    public void suggest_afterSkippedToken_shouldSuggest() {
        givenGrammar("r: 'A' 'B'", "WS: [ \\t] -> skip").whenInput("A ").thenExpect("B");
    }

    @Test
    public void suggest_beforeSkippedToken_shouldSuggest() {
        givenGrammar("r: 'A' 'B'", "WS: [ \\t] -> skip").whenInput("A").thenExpect("B");
    }

    @Test
    public void suggest_whenCompletionIsWildcard_shouldNotSuggest() {
        givenGrammar("r: A", "A: 'A'*").whenInput("").thenExpect();
    }

    @Test
    public void suggest_whenCompletionIsPlus_shouldSuggestOne() {
        givenGrammar("r: A", "A: 'A'+").whenInput("").thenExpect("A");
    }

    @Test
    public void suggest_whenWildcardInMiddleOfToken_shouldSuggestWithoutIt() {
        givenGrammar("r: A", "A: 'A' 'B'* 'C'").whenInput("A").thenExpect("C");
    }

    @Test
    public void suggest_whenPlusInMiddleOfToken_shouldSuggestWithOneInstance() {
        givenGrammar("r: A", "A: 'A' 'B'+ 'C'").whenInput("A").thenExpect("BC");
    }

    @Test
    public void suggest_whenCompletionIsAFragment_shouldNotSuggest() {
        givenGrammar("r: 'A' B", "fragment B: 'B'").whenInput("A").thenExpect();
    }

    @Test
    public void suggest_withTwoRulesOneMatching_shouldSuggestMatchingRule() {
        givenGrammar("r0: r1 | r2", "r1: 'AB'", "r2: 'CD'").whenInput("A").thenExpect("B");
    }

    @Test
    public void suggest_withTwoRulesBothMatching_shouldSuggestBoth() {
        givenGrammar("r0: r1 | r2", "r1: 'AB'", "r2: 'AC'").whenInput("A").thenExpect("B", "C");
    }

    @Test
    public void suggest_TokenMatchButNoParserRuleMatch_shouldNotSuggest() {
        givenGrammar("r0: 'A' 'B'").whenInput("B").thenExpect();
    }

   @Test
    public void suggest_withSecondRuleMatching_shouldNotSuggest() {
        givenGrammar("r0: r1 | r2", "r1: 'AB'", "r2: 'CD' 'EF'").whenInput("CD").thenExpect("EF");
    }

    @Test
    public void suggest_withSecondRuleMatchingAndNoNextToken_shouldNotSuggest() {
        givenGrammar("r0: r1 | r2", "r1: 'AB'", "r2: 'CD'").whenInput("CD").thenExpect();
    }

    @Test
    public void suggest_withPartialMatchForTwoTokens_shouldSuggest() {
        givenGrammar("r0: r1+", "r1: 'ABC' | 'ABCDE'").whenInput("AB").thenExpect("C", "CDE");
    }

    @Test
    public void suggest_withTwoTokenAlternativesInSameRule_shouldSuggest() {
        givenGrammar("r0: r1+", "r1: 'ABC' | 'XYZ'").whenInput("ABC").thenExpect("ABC", "XYZ");
    }
    
    @Test
    public void suggest_withTokensUsedInReverseOrder_shouldSuggest() {
        givenGrammar("r: F F F E D ", "C: 'c'", "D: 'd'", "E: 'e'", "F: 'f'").whenInput("fff").thenExpect("e");
    }

    @Test
    public void suggest_withSkippedToken_shouldSuggest() {
        givenGrammar("r: A B", "A: 'a'", "B: 'b'", "SP: ' ' -> skip").whenInput("a ").thenExpect("b");
    }

    @Test
    public void suggest_withHiddenChannelToken_shouldSuggest() {
        givenGrammar("r: A B", "A: 'a'", "B: 'b'", "SP: ' ' -> channel(HIDDEN)").whenInput("a ").thenExpect("b");
    }
    
    /**
     * @see https://github.com/oranoran/antlr4-autosuggest-js/issues/1
     */
    @Test
    public void suggest_js_issue1() {
      givenGrammar("varDecl: type ID '=' NUMBER ';'", "type: 'float' | 'int'", "ID: LETTER (LETTER | [0-9])*", "fragment LETTER : [a-zA-Z]", "NUMBER: DIGIT+", "fragment DIGIT : [0-9]", "SPACES: [ \\u000B\\t\\r\\n] -> channel(HIDDEN)").whenInput("int a").thenExpect("=");
    }

    /**
     * @see https://github.com/oranoran/antlr4-autosuggest-js/issues/6
     */
    @Test
    public void suggest_js_issue6() {
        givenGrammar("clause: clause AND clause | action", "action: 'action'", "AND: 'AND'").whenInput("action AND").thenExpect();
    }

    @Test
    public void suggest_withRecursiveRule_shouldFollowTransitionOnce_andNotCauseStackOverflow() {
        givenGrammar("a: b | a a", "b: 'B'").whenInput("B").thenExpect("B");
    }

    @Test
    public void suggest_withDefaultCasePreference_shouldSuggestBoth() {
        givenGrammar("r: AB", "AB: A B", "fragment A: 'A' | 'a'", "fragment B: 'B' | 'b'").whenInput("").thenExpect("ab", "AB", "aB", "Ab");
    }

    @Test
    public void suggest_withNullCasePreference_shouldSuggestBoth() {
        givenGrammar("r: AB", "AB: A B", "fragment A: 'A' | 'a'", "fragment B: 'B' | 'b'").withCasePreference(null).whenInput("").thenExpect("ab", "AB", "aB", "Ab");
    }

    @Test
    public void suggest_withExplicitNoCasePreference_shouldSuggestBoth() {
        givenGrammar("r: AB", "AB: A B", "fragment A: 'A' | 'a'", "fragment B: 'B' | 'b'").withCasePreference(BOTH).whenInput("").thenExpect("ab", "AB", "aB", "Ab");
    }

    @Test
    public void suggest_withUppercasePreference_shouldSuggestJustUpper() {
        givenGrammar("r: AB", "AB: A B", "fragment A: 'A' | 'a'", "fragment B: 'B' | 'b'").withCasePreference(UPPER).whenInput("").thenExpect("AB");
    }

    @Test
    public void suggest_withLowercasePreference_shouldSuggestJustLower() {
        givenGrammar("r: AB", "AB: A B", "fragment A: 'A' | 'a'", "fragment B: 'B' | 'b'").withCasePreference(LOWER).whenInput("").thenExpect("ab");
    }

    @Test
    public void suggest_withEofAfterOptional_shouldSuggest() {
        givenGrammar("r: A B? EOF", "A: 'A'", "B: 'B'").whenInput("A").thenExpect("B");
    }

    // @Test
    // public void suggest_withMultipleParseOptions_shouldSuggestAll() {
    // // Currently failing due to weird AST created by antlr4. Parser state 11
    // // has B completion token, while lexer state 11 actually generates C.
    // givenGrammar("r0: r1 | r1; r1: ('A' 'B') 'C'", "r2: 'A' ('B'
    // 'C')").whenInput("A").thenExpect("B", "BC");
    // }

    private AutoSuggesterTest givenGrammar(String... grammarLines) {
        this.lexerAndParserFactory = loadGrammar(grammarLines);
        printGrammarAtnIfNeeded();
        return this;
    }

    private AutoSuggesterTest withCasePreference(CasePreference casePreference) {
        this.casePreference = casePreference;
        return this;
    }

    /*
     * Used for testing with generated grammars, e.g. for checking out reported issues, before coming up with a more
     * focused test
     */
    protected AutoSuggesterTest givenGrammar(Class<? extends Lexer> lexerClass, Class<? extends Parser> parserClass) {
        this.lexerAndParserFactory = new ReflectionLexerAndParserFactory(lexerClass, parserClass);
        printGrammarAtnIfNeeded();
        return this;
    }

    private void printGrammarAtnIfNeeded() {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        if (!logger.isDebugEnabled()) {
            return;
        }
        Lexer lexer = this.lexerAndParserFactory.createLexer(null);
        Parser parser = this.lexerAndParserFactory.createParser(null);
        String header = "\n===========  PARSER ATN  ====================\n";
        String middle = "===========  LEXER ATN   ====================\n";
        String footer = "===========  END OF ATN  ====================";
        String parserAtn = AtnFormatter.printAtnFor(parser);
        String lexerAtn = AtnFormatter.printAtnFor(lexer);
        logger.debug(header + parserAtn + middle + lexerAtn + footer);
    }

    private AutoSuggesterTest whenInput(String input) {
        AutoSuggester suggester = new AutoSuggester(this.lexerAndParserFactory, input);
        suggester.setCasePreference(this.casePreference);
        this.suggestedCompletions = suggester.suggestCompletions();
        return this;
    }

    private void thenExpect(String... expectedCompletions) {
        assertThat(this.suggestedCompletions, containsInAnyOrder(expectedCompletions));
    }

    private LexerAndParserFactory loadGrammar(String... grammarlines) {
        String firstLine = "grammar testgrammar;\n";
        String grammarText = firstLine + StringUtils.join(Arrays.asList(grammarlines), ";\n") + ";\n";
        LexerGrammar lg;
        try {
            lg = new LexerGrammar(grammarText);
            Grammar g = new Grammar(grammarText);
            return new LexerAndParserFactory() {

                @Override
                public Parser createParser(TokenStream tokenStream) {
                    return g.createParserInterpreter(tokenStream);
                }

                @Override
                public Lexer createLexer(CharStream input) {
                    return lg.createLexerInterpreter(input);
                }
            };
        } catch (RecognitionException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
