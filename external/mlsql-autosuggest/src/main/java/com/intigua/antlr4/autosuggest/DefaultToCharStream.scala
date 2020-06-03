package com.intigua.antlr4.autosuggest

import java.io.StringReader

import org.antlr.v4.runtime.{CharStream, CharStreams}
import tech.mlsql.atuosuggest

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultToCharStream extends ToCharStream {
  override def toCharStream(text: String): CharStream = {
    CharStreams.fromReader(new StringReader(text))

  }
}

class RawSQLToCharStream extends ToCharStream {
  override def toCharStream(text: String): CharStream = {
    new atuosuggest.UpperCaseCharStream(CharStreams.fromString(text))
  }
}
