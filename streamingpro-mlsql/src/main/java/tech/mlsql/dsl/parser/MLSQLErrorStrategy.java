package tech.mlsql.dsl.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.misc.IntervalSet;

/**
 * 2019-07-04 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLErrorStrategy extends DefaultErrorStrategy {

    @Override
    public void sync(Parser recognizer) throws RecognitionException {
        ATNState s = recognizer.getInterpreter().atn.states.get(recognizer.getState());
//		System.err.println("sync @ "+s.stateNumber+"="+s.getClass().getSimpleName());
        // If already recovering, don't try to sync
        if (inErrorRecoveryMode(recognizer)) {
            return;
        }

        TokenStream tokens = recognizer.getInputStream();
        int la = tokens.LA(1);

        // try cheaper subset first; might get lucky. seems to shave a wee bit off
        if ( recognizer.getATN().nextTokens(s).contains(la) || la==Token.EOF ) return;

        // Return but don't end recovery. only do that upon valid token match
        if (recognizer.isExpectedToken(la)) {
            return;
        }

        switch (s.getStateType()) {
            case ATNState.BLOCK_START:
            case ATNState.STAR_BLOCK_START:
            case ATNState.PLUS_BLOCK_START:
            case ATNState.STAR_LOOP_ENTRY:
                // report error and recover if possible
                if ( singleTokenDeletion(recognizer)!=null ) {
                    return;
                }

                throw new InputMismatchException(recognizer);

            case ATNState.PLUS_LOOP_BACK:
            case ATNState.STAR_LOOP_BACK:
//			System.err.println("at loop back: "+s.getClass().getSimpleName());
                reportUnwantedToken(recognizer);
                IntervalSet expecting = recognizer.getExpectedTokens();
                IntervalSet whatFollowsLoopIterationOrRule =
                        expecting.or(getErrorRecoverySet(recognizer));
                consumeUntil(recognizer, whatFollowsLoopIterationOrRule);
                break;

            default:
                // do nothing if we can't identify the exact kind of ATN state
                break;
        }
    }
}
