package tech.mlsql.test

import org.apache.spark.sql.{DataFrameWriter, Row}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import tech.mlsql.common.utils.lang.sc.ScalaReflect
import scala.collection.JavaConversions._

/**
 * 10/11/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    val jack1 = new Jack()
    val jack2 = new Jack2()
    val extraOptions = ScalaReflect.fromInstance[BaseJack](jack2)
      .method("extraOptions").invoke()
      .asInstanceOf[{def toMap[T, U](implicit ev: _ <:< (T, U)): scala.collection.immutable.Map[T, U] }]

//    ScalaReflect.fromInstance[BaseJack](jack1)
//      .method("extraOptions").invoke().getClass.getMethods.map(item=>println(item))
//    println("===")
//    ScalaReflect.fromInstance[BaseJack](jack2)
//      .method("extraOptions").invoke().getClass.getMethods.map(item=>println(item))
    println(extraOptions.toMap)
  }
}

class BaseJack {
  
}
class Jack extends BaseJack {
  private var extraOptions = CaseInsensitiveMap[String](Map.empty)
  extraOptions.toMap
}

class Jack2 extends BaseJack{
  private var extraOptions = scala.collection.mutable.HashMap[String, String]()
  extraOptions.toMap
}
