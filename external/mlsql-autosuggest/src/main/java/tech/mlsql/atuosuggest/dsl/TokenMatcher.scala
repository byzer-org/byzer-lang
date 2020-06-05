package tech.mlsql.atuosuggest.dsl

import org.antlr.v4.runtime.Token

import scala.collection.mutable.ArrayBuffer

/**
 * 4/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TokenMatcher(tokens: List[Token], val start: Int) {
  val foods = ArrayBuffer[FoodWrapper]()
  var cacheResult = -2

  def eat(food: Food*) = {
    foods += FoodWrapper(AndOrFood(food.toList, true), false)
    this
  }

  // find the first match 
  def index(_foods: Array[Food]) = {
    if (foods.size != 0) {
      throw new RuntimeException("eat/optional/asStart should not before index")
    }
    var targetIndex = -1
    (start until tokens.size).foreach { idx =>
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

  private def matchToken(food: Food, currentIndex: Int) = {
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

  def build: TokenMatcher = {
    var currentIndex = start
    var isFail = false


    foods.map { foodw =>
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
    val targetIndex = if (isFail) -1 else currentIndex
    cacheResult = targetIndex
    this
  }

  def get = {
    if (this.cacheResult == -2) this.build
    this.cacheResult
  }

  def isSuccess = {
    if (this.cacheResult == -2) this.build
    this.cacheResult != -1
  }
}

object TokenMatcher {
  def apply(tokens: List[Token], start: Int): TokenMatcher = new TokenMatcher(tokens, start)

  def resultMatcher(tokens: List[Token], start: Int, stop: Int) = {
    val temp = new TokenMatcher(tokens, start)
    temp.cacheResult = stop
    temp
  }
}

case class Food(name: Option[String], tp: Int)

case class FoodWrapper(foods: AndOrFood, var optional: Boolean)

case class AndOrFood(foods: List[Food], var and: Boolean) {
  def count = {
    if (and) foods.size
    else 1
  }
}

