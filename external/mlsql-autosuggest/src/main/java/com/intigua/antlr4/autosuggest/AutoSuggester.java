package com.intigua.antlr4.autosuggest;

import com.intigua.antlr4.autosuggest.LexerWrapper.TokenizationResult;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.SetTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Suggests completions for given text, using a given ANTLR4 grammar.
 */
public class AutoSuggester {
    private static final Logger logger = LoggerFactory.getLogger(AutoSuggester.class);

    private final ParserWrapper parserWrapper;
    private final LexerWrapper lexerWrapper;
    private final String input;
    private final Set<String> collectedSuggestions = new HashSet<>();

    private List<? extends Token> inputTokens;
    private String untokenizedText = "";
    private String indent = "";
    private CasePreference casePreference = CasePreference.BOTH;

    private Map<ATNState, Integer> parserStateToTokenListIndexWhereLastVisited = new HashMap<>();

    public AutoSuggester(LexerAndParserFactory lexerAndParserFactory, String input) {
        this.lexerWrapper = new LexerWrapper(lexerAndParserFactory, new DefaultToCharStream());
        this.parserWrapper = new ParserWrapper(lexerAndParserFactory, lexerWrapper.getVocabulary());
        this.input = input;
    }

    public void setCasePreference(CasePreference casePreference) {
        this.casePreference = casePreference;
    }

    public Collection<String> suggestCompletions() {
        tokenizeInput();
        runParserAtnAndCollectSuggestions();
        return collectedSuggestions;
    }

    private void tokenizeInput() {
        TokenizationResult tokenizationResult = lexerWrapper.tokenizeNonDefaultChannel(this.input);
        this.inputTokens = tokenizationResult.tokens;
        this.untokenizedText = tokenizationResult.untokenizedText;
        if (logger.isDebugEnabled()) {
            logger.debug("TOKENS FOUND IN FIRST PASS:");
            for (Token token : this.inputTokens) {
                logger.debug(token.toString());
            }
        }
    }

    private void runParserAtnAndCollectSuggestions() {
        ATNState initialState = this.parserWrapper.getAtnState(0);
        logger.debug("Parser initial state: " + initialState);
        parseAndCollectTokenSuggestions(initialState, 0);
    }

    /**
     * Recursive through the parser ATN to process all tokens. When successful (out of tokens) - collect completion
     * suggestions.
     */
    private void parseAndCollectTokenSuggestions(ATNState parserState, int tokenListIndex) {
        indent = indent + "  ";
        if (didVisitParserStateOnThisTokenIndex(parserState, tokenListIndex)) {
            logger.debug(indent + "State " + parserState + " had already been visited while processing token "
                    + tokenListIndex + ", backtracking to avoid infinite loop.");
            return;
        }
        Integer previousTokenListIndexForThisState = setParserStateLastVisitedOnThisTokenIndex(parserState, tokenListIndex);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(indent + "State: " + parserWrapper.toString(parserState));
                logger.debug(indent + "State available transitions: " + parserWrapper.transitionsStr(parserState));
            }

            if (!haveMoreTokens(tokenListIndex)) { // stop condition for recursion
                suggestNextTokensForParserState(parserState);
                return;
            }
            for (Transition trans : parserState.getTransitions()) {
                if (trans.isEpsilon()) {
                    handleEpsilonTransition(trans, tokenListIndex);
                } else if (trans instanceof AtomTransition) {
                    handleAtomicTransition((AtomTransition) trans, tokenListIndex);
                } else {
                    handleSetTransition((SetTransition) trans, tokenListIndex);
                }
            }
        } finally {
            indent = indent.substring(2);
            setParserStateLastVisitedOnThisTokenIndex(parserState, previousTokenListIndexForThisState);
        }
    }

    private boolean didVisitParserStateOnThisTokenIndex(ATNState parserState, Integer currentTokenListIndex) {
        Integer lastVisitedThisStateAtTokenListIndex = parserStateToTokenListIndexWhereLastVisited.get(parserState);
        return currentTokenListIndex.equals(lastVisitedThisStateAtTokenListIndex);
    }

    private Integer setParserStateLastVisitedOnThisTokenIndex(ATNState parserState, Integer tokenListIndex) {
        if (tokenListIndex == null) {
            return parserStateToTokenListIndexWhereLastVisited.remove(parserState);
        } else {
            return parserStateToTokenListIndexWhereLastVisited.put(parserState, tokenListIndex);
        }
    }

    private boolean haveMoreTokens(int tokenListIndex) {
        return tokenListIndex < inputTokens.size();
    }

    private void handleEpsilonTransition(Transition trans, int tokenListIndex) {
        // Epsilon transitions don't consume a token, so don't move the index
        parseAndCollectTokenSuggestions(trans.target, tokenListIndex);
    }

    private void handleAtomicTransition(AtomTransition trans, int tokenListIndex) {
        Token nextToken = inputTokens.get(tokenListIndex);
        int nextTokenType = inputTokens.get(tokenListIndex).getType();
        boolean nextTokenMatchesTransition = (trans.label == nextTokenType);
        if (nextTokenMatchesTransition) {
            logger.debug(indent + "Token " + nextToken + " following transition: " + parserWrapper.toString(trans));
            parseAndCollectTokenSuggestions(trans.target, tokenListIndex + 1);
        } else {
            logger.debug(indent + "Token " + nextToken + " NOT following transition: " + parserWrapper.toString(trans));
        }
    }

    private void handleSetTransition(SetTransition trans, int tokenListIndex) {
        Token nextToken = inputTokens.get(tokenListIndex);
        int nextTokenType = nextToken.getType();
        for (int transitionTokenType : trans.label().toList()) {
            boolean nextTokenMatchesTransition = (transitionTokenType == nextTokenType);
            if (nextTokenMatchesTransition) {
                logger.debug(indent + "Token " + nextToken + " following transition: " + parserWrapper.toString(trans) + " to " + transitionTokenType);
                parseAndCollectTokenSuggestions(trans.target, tokenListIndex + 1);
            } else {
                logger.debug(indent + "Token " + nextToken + " NOT following transition: " + parserWrapper.toString(trans) + " to " + transitionTokenType);
            }
        }
    }

    private void suggestNextTokensForParserState(ATNState parserState) {
        Set<Integer> transitionLabels = new HashSet<>();
        fillParserTransitionLabels(parserState, transitionLabels, new HashSet<>());
        TokenSuggester tokenSuggester = new TokenSuggester(this.untokenizedText, lexerWrapper, this.casePreference);
        Collection<String> suggestions = tokenSuggester.suggest(transitionLabels);
        parseSuggestionsAndAddValidOnes(parserState, suggestions);
        logger.debug(indent + "WILL SUGGEST TOKENS FOR STATE: " + parserState);
    }

    private void fillParserTransitionLabels(ATNState parserState, Collection<Integer> result, Set<TransitionWrapper> visitedTransitions) {
        for (Transition trans : parserState.getTransitions()) {
            TransitionWrapper transWrapper = new TransitionWrapper(parserState, trans);
            if (visitedTransitions.contains(transWrapper)) {
                logger.debug(indent + "Not following visited " + transWrapper);
                continue;
            }
            if (trans.isEpsilon()) {
                try {
                    visitedTransitions.add(transWrapper);
                    fillParserTransitionLabels(trans.target, result, visitedTransitions);
                } finally {
                    visitedTransitions.remove(transWrapper);
                }
            } else if (trans instanceof AtomTransition) {
                int label = ((AtomTransition) trans).label;
                if (label >= 1) { // EOF would be -1
                    result.add(label);
                }
            } else if (trans instanceof SetTransition) {
                for (Interval interval : ((SetTransition) trans).label().getIntervals()) {
                    for (int i = interval.a; i <= interval.b; ++i) {
                        result.add(i);
                    }
                }
            }
        }
    }

    private void parseSuggestionsAndAddValidOnes(ATNState parserState, Collection<String> suggestions) {
        for (String suggestion : suggestions) {
            logger.debug("CHECKING suggestion: " + suggestion);
            Token addedToken = getAddedToken(suggestion);
            if (isParseableWithAddedToken(parserState, addedToken, new HashSet<TransitionWrapper>())) {
                collectedSuggestions.add(suggestion);
            } else {
                logger.debug("DROPPING non-parseable suggestion: " + suggestion);
            }
        }
    }

    private Token getAddedToken(String suggestedCompletion) {
        String completedText = this.input + suggestedCompletion;
        List<? extends Token> completedTextTokens = this.lexerWrapper.tokenizeNonDefaultChannel(completedText).tokens;
        if (completedTextTokens.size() <= inputTokens.size()) {
            return null; // Completion didn't yield whole token, could be just a token fragment
        }
        logger.debug("TOKENS IN COMPLETED TEXT: " + completedTextTokens);
        Token newToken = completedTextTokens.get(completedTextTokens.size() - 1);
        return newToken;
    }

    private boolean isParseableWithAddedToken(ATNState parserState, Token newToken, Set<TransitionWrapper> visitedTransitions) {
        if (newToken == null) {
            return false;
        }
        for (Transition parserTransition : parserState.getTransitions()) {
            if (parserTransition.isEpsilon()) { // Recurse through any epsilon transitionsStr
                TransitionWrapper transWrapper = new TransitionWrapper(parserState, parserTransition);
                if (visitedTransitions.contains(transWrapper)) {
                    continue;
                }
                visitedTransitions.add(transWrapper);
                try {
                    if (isParseableWithAddedToken(parserTransition.target, newToken, visitedTransitions)) {
                        return true;
                    }
                } finally {
                    visitedTransitions.remove(transWrapper);
                }
            } else if (parserTransition instanceof AtomTransition) {
                AtomTransition parserAtomTransition = (AtomTransition) parserTransition;
                int transitionTokenType = parserAtomTransition.label;
                if (transitionTokenType == newToken.getType()) {
                    return true;
                }
            } else if (parserTransition instanceof SetTransition) {
                SetTransition parserSetTransition = (SetTransition) parserTransition;
                for (int transitionTokenType : parserSetTransition.label().toList()) {
                    if (transitionTokenType == newToken.getType()) {
                        return true;
                    }
                }
            } else {
                throw new IllegalStateException("Unexpected: " + parserWrapper.toString(parserTransition));
            }
        }
        return false;
    }


}
