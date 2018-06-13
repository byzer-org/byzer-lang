package org.apache.spark.util

import org.apache.spark.util.random.XORShiftRandom

/**
  * Created by allwefantasy on 5/2/2018.
  */
class WowXORShiftRandom {
  val random = new XORShiftRandom()

  def nextDouble = {
    random.nextDouble()
  }

}

object WowXORShiftRandom {
  def main(args: Array[String]): Unit = {
    val random = new WowXORShiftRandom()
    (0 until 1000).foreach { f =>
      println(random.nextDouble)
    }
  }
}
