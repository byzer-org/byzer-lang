package com.intigua.antlr4.autosuggest

import tech.mlsql.atuosuggest.statement.LexerUtils
import tech.mlsql.atuosuggest.{TokenPos, TokenPosType}

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class LexerUtilsTest extends BaseTest {

  test(" load [cursor]hive.`` as -- jack") {
    assert(LexerUtils.toTokenPos(tokens, 3, 6) == TokenPos(0, TokenPosType.NEXT, 0))

  }
  test(" load h[cursor]ive.`` as -- jack") {
    assert(LexerUtils.toTokenPos(tokens.toList, 3, 7) == TokenPos(1, TokenPosType.CURRENT, 1))
  }

  test("[cursor] load hive.`` as -- jack") {
    assert(LexerUtils.toTokenPos(tokens.toList, 3, 0) == TokenPos(-1, TokenPosType.NEXT, 0))
  }
  test(" load hive.`` as -- jack [cursor]") {
    assert(LexerUtils.toTokenPos(tokens.toList, 3, 23) == TokenPos(4, TokenPosType.NEXT, 0))
  }
  
}
