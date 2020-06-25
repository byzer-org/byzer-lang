package tech.mlsql.autosuggest.dsl

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 *
 */
class TokenMatcher(tokens: List[Token], val start: Int) {
  val foods = ArrayBuffer[FoodWrapper]()
  var cacheResult = -2
  private var direction: String = MatcherDirection.FORWARD

  def forward = {
    assert(foods.size == 0, "this function should be invoke before eat")
    direction = MatcherDirection.FORWARD
    this
  }

  def back = {
    assert(foods.size == 0, "this function should be invoke before eat")
    direction = MatcherDirection.BACK
    this
  }

  def eat(food: Food*) = {
    foods += FoodWrapper(AndOrFood(food.toList, true), false)
    this
  }

  def eatOneAny = {
    foods += FoodWrapper(AndOrFood(List(Food(None, -2)), true), false)
    this
  }

  /**
   *
   * 一直前进 直到遇到我们需要的,成功返回最后的index值，否则返回-1
   */
  def orIndex(_foods: Array[Food], upperBound: Int = tokens.size) = {
    if (foods.size != 0) {
      throw new RuntimeException("eat/optional/asStart should not before index")
    }
    direction match {
      case MatcherDirection.FORWARD =>
        var targetIndex = -1
        (start until upperBound).foreach { idx =>
          if (targetIndex == -1) {
            // step by step until success
            var matchValue = -1
            _foods.zipWithIndex.foreach { case (food, _) =>
              if (matchValue == -1 && matchToken(food, idx) != -1) {
                matchValue = 0
              }
            }
            if (matchValue != -1) {
              targetIndex = idx
            }
          }

        }
        targetIndex
      case MatcherDirection.BACK =>
        var _start = start
        var targetIndex = -1
        while (_start >= 0) {
          if (targetIndex == -1) {
            // step by step until success
            var matchValue = -1
            _foods.zipWithIndex.foreach { case (food, _) =>
              if (matchValue == -1 && matchToken(food, _start) != -1) {
                matchValue = 0
              }
            }
            if (matchValue != -1) {
              targetIndex = _start
            }
          }
          _start = _start - 1
        }
        targetIndex
    }

  }

  // find the first match 
  def index(_foods: Array[Food], upperBound: Int = tokens.size) = {
    if (foods.size != 0) {
      throw new RuntimeException("eat/optional/asStart should not before index")
    }
    assert(direction == MatcherDirection.FORWARD, "index only support forward")
    var targetIndex = -1
    (start until upperBound).foreach { idx =>
      if (targetIndex == -1) {
        // step by step until success
        var matchValue = 0
        _foods.zipWithIndex.foreach { case (food, idx2) =>
          if (matchValue == 0 && matchToken(food, idx + idx2) == -1) {
            matchValue = -1
          }
        }
        if (matchValue != -1) {
          targetIndex = idx
        }
      }

    }
    targetIndex

  }

  def asStart(food: Food, offset: Int = 0) = {
    if (foods.size != 0) {
      throw new RuntimeException("eat/optional should not before asStart")
    }
    var targetIndex = -1
    (start until tokens.size).foreach { idx =>
      if (targetIndex == -1) {
        val index = matchToken(food, idx)
        if (index != -1) {
          targetIndex = index
        }
      }

    }
    TokenMatcher(tokens, targetIndex + offset)
  }

  def optional = {
    foods.lastOption.foreach(_.optional = true)
    this
  }

  private def matchToken(food: Food, currentIndex: Int): Int = {
    if (currentIndex < 0) return -1
    if (currentIndex >= tokens.size) return -1
    if (food.tp == -2) {
      return currentIndex
    }
    food.name match {
      case Some(name) => if (tokens(currentIndex).getType == food.tp && tokens(currentIndex).getText == name) {
        currentIndex
      } else -1
      case None =>
        if (tokens(currentIndex).getType == food.tp) {
          currentIndex
        } else -1
    }
  }

  private def forwardBuild: TokenMatcher = {
    var currentIndex = start
    var isFail = false


    foods.foreach { foodw =>

      if (currentIndex >= tokens.size && !foodw.optional) {
        isFail = true
      } else {
        val stepSize = foodw.foods.count
        var matchValue = 0
        foodw.foods.foods.zipWithIndex.foreach { case (food, idx) =>
          if (matchValue == 0 && matchToken(food, currentIndex + idx) == -1) {
            matchValue = -1
          }
        }
        if (foodw.optional) {
          if (matchValue != -1) {
            currentIndex = currentIndex + stepSize
          }
        } else {
          if (matchValue != -1) {
            currentIndex = currentIndex + stepSize

          } else {
            //mark fail
            isFail = true
          }
        }
      }
    }
    
    val targetIndex = if (isFail) -1 else currentIndex
    cacheResult = targetIndex
    this
  }

  private def backBuild: TokenMatcher = {
    var currentIndex = start
    var isFail = false


    foods.foreach { foodw =>
      // if out of bound then mark fail
      if (currentIndex <= -1 && !foodw.optional) {
        isFail = true
      } else {
        val stepSize = foodw.foods.count
        var matchValue = 0
        foodw.foods.foods.zipWithIndex.foreach { case (food, idx) =>
          if (matchValue == 0 && matchToken(food, currentIndex - idx) == -1) {
            matchValue = -1
          }
        }
        if (foodw.optional) {
          if (matchValue != -1) {
            currentIndex = currentIndex - stepSize
          }
        } else {
          if (matchValue != -1) {
            currentIndex = currentIndex - stepSize

          } else {
            //mark fail
            isFail = true
          }
        }
      }

    }

    if (!isFail && currentIndex == -1) {
      currentIndex = 0
    }
    val targetIndex = if (isFail) -1 else currentIndex
    cacheResult = targetIndex
    this
  }

  def build: TokenMatcher = {
    direction match {
      case MatcherDirection.FORWARD =>
        forwardBuild
      case MatcherDirection.BACK =>
        backBuild
    }
  }

  def get = {
    if (this.cacheResult == -2) this.build
    this.cacheResult
  }

  def isSuccess = {
    if (this.cacheResult == -2) this.build
    this.cacheResult != -1
  }

  def getMatchTokens = {
    direction match {
      case MatcherDirection.BACK => tokens.slice(get + 1, start + 1)
      case MatcherDirection.FORWARD => tokens.slice(start, get)
    }

  }
}

object MatcherDirection {
  val FORWARD = "forward"
  val BACK = "back"
}

object TokenTypeWrapper {
  val LEFT_BRACKET = SqlBaseLexer.T__0 //(
  val RIGHT_BRACKET = SqlBaseLexer.T__1 //)
  val COMMA = SqlBaseLexer.T__2 //,
  val DOT = SqlBaseLexer.T__3 //.
  val LEFT_SQUARE_BRACKET = SqlBaseLexer.T__7 //[
  val RIGHT_SQUARE_BRACKET = SqlBaseLexer.T__8 //]
  val COLON = SqlBaseLexer.T__9 //:

  val LIST = List(LEFT_BRACKET, RIGHT_BRACKET, COMMA, DOT, LEFT_SQUARE_BRACKET, RIGHT_SQUARE_BRACKET, COLON)
  val MAP = LIST.map((_, 1)).toMap
}

object MLSQLTokenTypeWrapper {
  val DOT = DSLSQLLexer.T__0
}

object TokenMatcher {
  def apply(tokens: List[Token], start: Int): TokenMatcher = new TokenMatcher(tokens, start)

  def resultMatcher(tokens: List[Token], start: Int, stop: Int) = {
    val temp = new TokenMatcher(tokens, start)
    temp.cacheResult = stop
    temp
  }

  def SQL_SPLITTER_KEY_WORDS = List(
    SqlBaseLexer.SELECT,
    SqlBaseLexer.FROM,
    SqlBaseLexer.JOIN,
    SqlBaseLexer.WHERE,
    SqlBaseLexer.GROUP,
    SqlBaseLexer.ON,
    SqlBaseLexer.BY,
    SqlBaseLexer.LIMIT,
    SqlBaseLexer.ORDER
  )
}

case class Food(name: Option[String], tp: Int)

case class FoodWrapper(foods: AndOrFood, var optional: Boolean)

case class AndOrFood(foods: List[Food], var and: Boolean) {
  def count = {
    if (and) foods.size
    else 1
  }
}

object TokenWalker {
  def apply(tokens: List[Token], start: Int): TokenWalker = new TokenWalker(tokens, start)
}

class TokenWalker(tokens: List[Token], start: Int) {

  var currentToken: Option[Token] = Option(tokens(start))
  var currentIndex = start

  def nextSafe: TokenWalker = {
    val token = if ((currentIndex + 1) < tokens.size) {
      currentIndex += 1
      Option(tokens(currentIndex))
    } else None
    currentToken = token
    this
  }

  def range: TokenCharRange = {
    if (currentToken.isEmpty) return TokenCharRange(-1, -1)
    val start = currentToken.get.getCharPositionInLine
    val end = currentToken.get.getCharPositionInLine + currentToken.get.getText.size
    TokenCharRange(start, end)
  }
}

case class TokenCharRange(start: Int, end: Int)

