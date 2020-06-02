package com.intigua.antlr4.autosuggest;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

public class ReflectionLexerAndParserFactoryTest {

    @Test
    public void create_succeeds() {
        LexerAndParserFactory factory = new ReflectionLexerAndParserFactory(TestGrammarLexer.class,
                TestGrammarParser.class);
        Lexer createdLexer = factory.createLexer(null);
        Parser createdParser = factory.createParser(null);
        assertThat(createdLexer, instanceOf(TestGrammarLexer.class));
        assertThat(createdParser, instanceOf(TestGrammarParser.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_withLexerConstructorFailure_shouldFail() {
        LexerAndParserFactory factory = new ReflectionLexerAndParserFactory(FailingLexer.class,
                TestGrammarParser.class);
        factory.createLexer(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_withParserMissingConstructorFailure_shouldFail() {
        LexerAndParserFactory factory = new ReflectionLexerAndParserFactory(TestGrammarLexer.class,
                FailingParser.class);
        factory.createParser(null);
    }

    static class TestGrammarLexer extends Lexer {
        public CharStream input;

        public TestGrammarLexer(CharStream input) {
            this.input = input;
        }

        @Override
        public String[] getRuleNames() {
            return null;
        }

        @Override
        public String getGrammarFileName() {
            return null;
        }

        @Override
        public ATN getATN() {
            return null;
        }

    };

    static class TestGrammarParser extends Parser {

        public TestGrammarParser(TokenStream tokenStream) {
            super(tokenStream);
        }

        @Override
        public String[] getTokenNames() {
            return null;
        }

        @Override
        public String[] getRuleNames() {
            return null;
        }

        @Override
        public String getGrammarFileName() {
            return null;
        }

        @Override
        public ATN getATN() {
            return null;
        }
    }

    static class FailingLexer extends TestGrammarLexer {
        public FailingLexer(CharStream input) {
            super(input);
            throw new NumberFormatException();
        }
    };

    static class FailingParser extends TestGrammarParser {
        public FailingParser() throws Exception {
            super(null);
        }
    };
}
