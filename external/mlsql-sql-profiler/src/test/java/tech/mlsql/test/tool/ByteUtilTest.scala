package tech.mlsql.test.tool

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.tool.ZOrderingBytesUtil

/**
 * 31/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ByteUtilTest extends FunSuite with BeforeAndAfterAll {
  test("toString") {
    //只需要支持证正数
    val a = 3L
    val b = 4L
    val c = 12L
    assert(ZOrderingBytesUtil.toString(ZOrderingBytesUtil.toBytes(a)).stripSuffix(" ") == "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000011")
    assert(ZOrderingBytesUtil.toString(ZOrderingBytesUtil.toBytes(b)).stripSuffix(" ") == "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000100")
    assert(ZOrderingBytesUtil.toString(ZOrderingBytesUtil.toBytes(c)).stripSuffix(" ") == "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00001100")
  }

  test("func"){
    println(ZOrderingBytesUtil.toString(ZOrderingBytesUtil.toBytes(-1)))
  }

  test("intTo8Byte") {
    val a = 3
    val aStr = ZOrderingBytesUtil.toString(ZOrderingBytesUtil.intTo8Byte(a))
    assert(aStr == "00000000 00000000 00000000 00000000 10000000 00000000 00000000 00000011 ")

    val b = -3
    val bStr = ZOrderingBytesUtil.toString(ZOrderingBytesUtil.intTo8Byte(b))
    assert(bStr == "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000011 ")
  }

  test("longTo8Byte") {
    val a = 3L
    val aStr = ZOrderingBytesUtil.toString(ZOrderingBytesUtil.longTo8Byte(a))
    assert(aStr == "10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000011 ")

    val b = -3L
    val bStr = ZOrderingBytesUtil.toString(ZOrderingBytesUtil.longTo8Byte(b))
    assert(bStr == "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000011 ")

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(3L), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(-3L), 0, 8) > 0)

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(-3L), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(3L), 0, 8) < 0)

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(-3L), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(-4L), 0, 8) > 0)
  }

  test("doubleTo8Byte") {
    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(3.1D), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(-3.1D), 0, 8) > 0)

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(3.2D), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(3.3D), 0, 8) < 0)

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.doubleTo8Byte(-3.2D), 0, 8,
      ZOrderingBytesUtil.doubleTo8Byte(-3.3D), 0, 8) > 0)
  }

  test("utf8To8Byte"){
    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.utf8To8Byte("abc"), 0, 8,
      ZOrderingBytesUtil.utf8To8Byte("bbc"), 0, 8) < 0)

    assert(ZOrderingBytesUtil.compareTo(
      ZOrderingBytesUtil.utf8To8Byte("abc"), 0, 8,
      ZOrderingBytesUtil.utf8To8Byte("abc"), 0, 8) == 0)
  }

  test("interleaveMulti8Byte"){
    val x1 = -7
    val y1 = 10
    val z1 = 1

    val x2 = 6
    val y2 = 18
    val z2 = 1

    val xyz1 = ZOrderingBytesUtil.interleaveMulti8Byte(
      Array[Array[Byte]](
        ZOrderingBytesUtil.intTo8Byte(x1),
        ZOrderingBytesUtil.intTo8Byte(y1),
        ZOrderingBytesUtil.intTo8Byte(z1)
      )
    )

    val xyz2 = ZOrderingBytesUtil.interleaveMulti8Byte(
      Array[Array[Byte]](
        ZOrderingBytesUtil.intTo8Byte(x2),
        ZOrderingBytesUtil.intTo8Byte(y2),
        ZOrderingBytesUtil.intTo8Byte(z2)
      )
    )
    assert(ZOrderingBytesUtil.compareTo(
      xyz1, 0, 24,
      xyz2, 0, 24) < 0)
    
  }
}
