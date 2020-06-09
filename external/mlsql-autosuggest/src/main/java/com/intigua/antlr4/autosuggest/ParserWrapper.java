package com.intigua.antlr4.autosuggest;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ParserWrapper {
    private static final Logger logger = LoggerFactory.getLogger(ParserWrapper.class);
    private final Vocabulary lexerVocabulary;
    
    private final ATN parserAtn;
    private final String[] parserRuleNames;

    public ParserWrapper(ParserFactory parserFactory, Vocabulary lexerVocabulary) {
        this.lexerVocabulary = lexerVocabulary;
        
        Parser parserForAtnOnly = parserFactory.createParser(null);
        this.parserAtn = parserForAtnOnly.getATN();
        this.parserRuleNames = parserForAtnOnly.getRuleNames();
        logger.debug("Parser rule names: " + StringUtils.join(parserForAtnOnly.getRuleNames(), ", "));
    }
    
    public String toString(ATNState parserState) {
        String ruleName = this.parserRuleNames[parserState.ruleIndex];
        return "*" + ruleName + "* " + parserState.getClass().getSimpleName() + " " + parserState;
    }

    public String toString(Transition t) {
        String nameOrLabel = t.getClass().getSimpleName();
        if (t instanceof AtomTransition) {
            nameOrLabel += ' ' + this.lexerVocabulary.getDisplayName(((AtomTransition) t).label);
        }
        return nameOrLabel + " -> " + toString(t.target);
    }

    public String transitionsStr(ATNState state) {
        Stream<Transition> transitionsStream = Arrays.asList(state.getTransitions()).stream();
        List<String> transitionStrings = transitionsStream.map(this::toString).collect(Collectors.toList());
        return StringUtils.join(transitionStrings, ", ");
    }

    public ATNState getAtnState(int stateNumber) {
        return parserAtn.states.get(stateNumber);
    }
}
