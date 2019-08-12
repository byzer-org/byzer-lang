package tech.mlsql.test

import org.scalatest.{FlatSpec, Matchers}
import tech.mlsql.common.utils.lang.sc.ScalaReflect

trait A1 {

}

private[test] class A extends A1 {
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

class ScalaReflectSpec extends FlatSpec with Matchers {
  it should " reflect method or filed correct" in {
    val a = new A()

    assert(ScalaReflect.
      fromInstance[A](a).
      method("a").
      invoke("jack") == "jack^")

    assert(ScalaReflect.
      fromObject[A]().
      method("b").
      invoke() == "h^")

    assert(ScalaReflect.
      fromObjectStr("tech.mlsql.test.A").
      method("b").
      invoke() == "h^")

    ScalaReflect.fromInstance[A](a).
      field("jack").
      invoke("^^^")

    assert(ScalaReflect.
      fromInstance[A](a).
      method("a").
      invoke("jack") == "jack^^^")
  }
}