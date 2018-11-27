package org.apache.spark

import org.antlr.v4.runtime.atn.ATNSimulator
import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer}
import org.apache.spark.internal.Logging

class MLSQLSyntaxErrorListener extends BaseErrorListener with Logging {
  override def syntaxError(recognizer: Recognizer[_, _],
                           offendingSymbol:
                           scala.Any,
                           line: Int,
                           charPositionInLine: Int,
                           msg: String,
                           e: RecognitionException): Unit = {
    throw new RuntimeException("MLSQL Parser error in [row:" + line + " column:" + charPositionInLine + "]  error msg: " + msg)
  }
}
