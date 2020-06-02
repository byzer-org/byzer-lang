package com.intigua.antlr4.autosuggest;

import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.SetTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Given an ATN state and the lexer ATN, suggests auto-completion texts.
 */
class TokenSuggester {
    private static final Logger logger = LoggerFactory.getLogger(TokenSuggester.class);

    private final LexerWrapper lexerWrapper;
    private final CasePreference casePreference;

    private final Set<String> suggestions = new TreeSet<String>();
    private final List<Integer> visitedLexerStates = new ArrayList<>();
    private String origPartialToken;

    public TokenSuggester(LexerWrapper lexerWrapper, String input) {
        this(input, lexerWrapper, CasePreference.BOTH);
    }

    public TokenSuggester(String origPartialToken, LexerWrapper lexerWrapper, CasePreference casePreference) {
        this.origPartialToken = origPartialToken;
        this.lexerWrapper = lexerWrapper;
        this.casePreference = casePreference;
    }

    public Collection<String> suggest(Collection<Integer> nextParserTransitionLabels) {
        logTokensUsedForSuggestion(nextParserTransitionLabels);
        for (int nextParserTransitionLabel : nextParserTransitionLabels) {
            int nextTokenRuleNumber = nextParserTransitionLabel - 1; // Count from 0 not from 1
            ATNState lexerState = this.lexerWrapper.findStateByRuleNumber(nextTokenRuleNumber);
            suggest("", lexerState, origPartialToken);
        }
        return suggestions;
//        return suggestions.stream().filter(s -> this.lexerWrapper.isValidSuggestion(input, s)).collect(Collectors.toList());
    }

    private void logTokensUsedForSuggestion(Collection<Integer> ruleIndices) {
        if (!logger.isDebugEnabled()) {
            return;
        }
        String ruleNames = ruleIndices.stream().map(r -> lexerWrapper.getRuleNames()[r-1]).collect(Collectors.joining(" "));
        logger.debug("Suggesting tokens for lexer rules: " + ruleNames, " ");
    }


    private void suggest(String tokenSoFar, ATNState lexerState, String remainingText) {
        logger.debug(
                "SUGGEST: tokenSoFar=" + tokenSoFar + " remainingText=" + remainingText + " lexerState=" + toString(lexerState));
        if (visitedLexerStates.contains(lexerState.stateNumber)) {
            return; // avoid infinite loop and stack overflow
        }
        visitedLexerStates.add(lexerState.stateNumber);
        try {
            Transition[] transitions = lexerState.getTransitions();
            boolean tokenNotEmpty = tokenSoFar.length() > 0;
            boolean noMoreCharactersInToken = (transitions.length == 0);
            if (tokenNotEmpty && noMoreCharactersInToken) {
                addSuggestedToken(tokenSoFar);
                return;
            }
            for (Transition trans : transitions) {
                suggestViaLexerTransition(tokenSoFar, remainingText, trans);
            }
        } finally {
            visitedLexerStates.remove(visitedLexerStates.size() - 1);
        }
    }

    private String toString(ATNState lexerState) {
        String ruleName = this.lexerWrapper.getRuleNames()[lexerState.ruleIndex];
        return ruleName + " " + lexerState.getClass().getSimpleName() + " " + lexerState;
    }

    private void suggestViaLexerTransition(String tokenSoFar, String remainingText, Transition trans) {
        if (trans.isEpsilon()) {
            suggest(tokenSoFar, trans.target, remainingText);
        } else if (trans instanceof AtomTransition) {
            String newTokenChar = getAddedTextFor((AtomTransition) trans);
            if (remainingText.isEmpty() || remainingText.startsWith(newTokenChar)) {
                logger.debug("LEXER TOKEN: " + newTokenChar + " remaining=" + remainingText);
                suggestViaNonEpsilonLexerTransition(tokenSoFar, remainingText, newTokenChar, trans.target);
            } else {
                logger.debug("NONMATCHING LEXER TOKEN: " + newTokenChar + " remaining=" + remainingText);
            }
        } else if (trans instanceof SetTransition) {
            List<Integer> symbols = ((SetTransition) trans).label().toList();
            for (Integer symbol : symbols) {
                char[] charArr = Character.toChars(symbol);
                String charStr = new String(charArr);
                boolean shouldIgnoreCase = shouldIgnoreThisCase(charArr[0], symbols); // TODO: check for non-BMP
                if (!shouldIgnoreCase && (remainingText.isEmpty() || remainingText.startsWith(charStr))) {
                    suggestViaNonEpsilonLexerTransition(tokenSoFar, remainingText, charStr, trans.target);
                }
            }
        }
    }

    private void suggestViaNonEpsilonLexerTransition(String tokenSoFar, String remainingText,
                                                     String newTokenChar, ATNState targetState) {
        String newRemainingText = (remainingText.length() > 0) ? remainingText.substring(1) : remainingText;
        suggest(tokenSoFar + newTokenChar, targetState, newRemainingText);
    }

    private void addSuggestedToken(String tokenToAdd) {
        String justTheCompletionPart = chopOffCommonStart(tokenToAdd, this.origPartialToken);
        suggestions.add(justTheCompletionPart);
    }

    private String chopOffCommonStart(String a, String b) {
        int charsToChopOff = Math.min(b.length(), a.length());
        return a.substring(charsToChopOff);
    }

    private String getAddedTextFor(AtomTransition transition) {
        return new String(Character.toChars(transition.label));
    }

    private boolean shouldIgnoreThisCase(char transChar, List<Integer> allTransChars) {
        if (this.casePreference == null) {
            return false;
        }
        switch(this.casePreference) {
        case BOTH:
            return false;
        case LOWER:
            return Character.isUpperCase(transChar) && allTransChars.contains((int) Character.toLowerCase(transChar));
        case UPPER:
            return Character.isLowerCase(transChar) && allTransChars.contains((int) Character.toUpperCase(transChar));
        default:
            return false;
        }
    }

}
