package com.intigua.antlr4.autosuggest;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.*;

import java.util.Set;
import java.util.TreeSet;

/**
 * Utility class that pretty-prints a textual tree representation of an ATN.
 */
public class AtnFormatter {

    public static String printAtnFor(Lexer lexer) {
        return new LexerAtnPrinter(lexer).atnToString();
    }

    public static String printAtnFor(Parser parser) {
        return new ParserAtnPrinter(parser).atnToString();
    }

    /**
     * Does most of the heavy lifting, leaving how specific transitions are printed to subclasses.
     */
    private abstract static class BaseAtnPrinter<R extends Recognizer<?, ?>> {
        private StringBuilder result = new StringBuilder();
        private Set<Integer> visitedStates = new TreeSet<Integer>();
        protected R recognizer;

        private BaseAtnPrinter(R recognizer) {
            this.recognizer = recognizer;
        }

        public String atnToString() {
            appendAtnTree();
            appendTableOfRuleToStartState();
            return result.toString();
        }

        private void appendAtnTree() {
            for (ATNState state : recognizer.getATN().states) {
                if (visitedStates.contains(state.stateNumber)) {
                    continue;
                }
                appendAtnSubtree("", "", state);
            }
        }

        private void appendAtnSubtree(String indent, String transStr, ATNState state) {
            String stateRuleName = (state.ruleIndex >= 0) ? recognizer.getRuleNames()[state.ruleIndex] : "";
            String stateClassName = state.getClass().getSimpleName();
            boolean visitedAlready = visitedStates.contains(state.stateNumber);
            String visitedTag = visitedAlready ? "*" : "";
            String stateStr = stateRuleName + " " + stateClassName + " " + state + visitedTag;
            result.append(indent + transStr + stateStr).append("\n");
            if (visitedAlready) {
                return;
            }
            visitedStates.add(state.stateNumber);
            {
                for (Transition trans : state.getTransitions()) {
                    String newTransStr = trans.toString();
                    if (trans instanceof AtomTransition) {
                        newTransStr = toString((AtomTransition) trans);
                    }
                    appendAtnSubtree(indent + "  ", " " + newTransStr + "-> ", trans.target);
                }
            }
        }

        private void appendTableOfRuleToStartState() {
            for (int i = 0; i < recognizer.getATN().ruleToStartState.length; ++i) {
                RuleStartState startState = recognizer.getATN().ruleToStartState[i];
                RuleStopState endState = recognizer.getATN().ruleToStopState[i];
                result.append(String.format("Rule %2d %-20s start: %d  stop: %d", i, recognizer.getRuleNames()[i], startState.stateNumber,
                        endState.stateNumber));
                result.append("\n");
            }
        }

        abstract protected String toString(AtomTransition trans);

    }

    private static class LexerAtnPrinter extends BaseAtnPrinter<Lexer> {
        private LexerAtnPrinter(Lexer lexer) {
            super(lexer);
        }

        protected String toString(AtomTransition trans) {
            int codePoint = trans.label().get(0);
            return "'" + Character.toChars(codePoint)[0] + "' ";
        }

    }

    private static class ParserAtnPrinter extends BaseAtnPrinter<Parser> {
        private ParserAtnPrinter(Parser parser) {
            super(parser);
        }

        protected String toString(AtomTransition trans) {
            String transDisplayName = recognizer.getVocabulary().getSymbolicName(trans.label);
            return transDisplayName + "(" + trans.label + ") ";
        }

    }

}
