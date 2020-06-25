package com.intigua.antlr4.autosuggest

import tech.mlsql.autosuggest.statement.LexerUtils
import tech.mlsql.autosuggest.{TokenPos, TokenPosType}

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

  test("select sum([cursor]) as t from table") {
    context.buildFromString("select sum() as t from table")
    assert(LexerUtils.toTokenPos(context.rawTokens, 1, 11) == TokenPos(2, TokenPosType.NEXT, 0))
  }

  test("select  from (select table2.abc as abc from table1 left join table2 on table1.column1 == table2.[cursor]) t1") {
    context.buildFromString("select  from (select table2.abc as abc from table1 left join table2 on table1.column1 == table2.) t1")
    assert(LexerUtils.toTokenPos(context.rawTokens, 1, 96) == TokenPos(21, TokenPosType.NEXT, 0))
  }

  test("select sum(abc[cursor]) as t from table") {
    context.buildFromString("select sum(abc) as t from table")
    assert(LexerUtils.toTokenPos(context.rawTokens, 1, 14) == TokenPos(3, TokenPosType.CURRENT, 3))
  }

  test("load csv.") {
    context.buildFromString("load csv.")
    assert(LexerUtils.toTokenPos(context.rawTokens, 1, 9) == TokenPos(2, TokenPosType.NEXT, 0))
  }

}
