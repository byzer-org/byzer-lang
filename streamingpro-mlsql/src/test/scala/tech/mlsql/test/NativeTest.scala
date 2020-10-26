package tech.mlsql.test

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.nativelib.runtime.MLSQLNativeRuntime

/**
 * 20/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class NativeTest  extends FunSuite with BeforeAndAfterAll {
  test("wow"){
     println(MLSQLNativeRuntime.funcLower("Dj"))
  }
}
