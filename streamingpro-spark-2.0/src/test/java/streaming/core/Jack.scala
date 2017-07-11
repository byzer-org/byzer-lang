package streaming.core

/**
  * Created by allwefantasy on 10/7/2017.
  */
class Jack {
  println(Thread.currentThread().getName)
  def echo(str: String) = {
    println("----"+Thread.currentThread().getName)
    str
  }
}
