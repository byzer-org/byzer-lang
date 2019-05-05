package org.apache.spark

import tech.mlsql.common.ScalaReflect

trait A1 {

}

private[spark] class A extends A1 {
  private val jack: String = "^"

  def a(hell: String) = {
    val b = hell + jack;
    b
  }
}

object A {
  def b = {
    new A().a("h")
  }

}

/**
  * 2019-04-26 WilliamZhu(allwefantasy@gmail.com)
  */
object Main {
  def main(args: Array[String]): Unit = {
    //    classOf[A].getDeclaredFields().foreach(f => println(f.getName))
    //    val field = classOf[A].getDeclaredField("jack")
    //    println(field)
    //
    val a = new A()

    println(ScalaReflect.fromInstance[A](a).method("a").invoke("jack"))
    println(ScalaReflect.fromObject[A]().method("b").invoke())
    println(ScalaReflect.fromObjectStr("org.apache.spark.A").method("b").invoke())

    println(ScalaReflect.fromInstance[A](a).field("jack").invoke("^^^"))
    println(ScalaReflect.fromInstance[A](a).method("a").invoke("jack"))


  }
}


