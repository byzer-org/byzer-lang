package tech.mlsql.tool

import java.io.ByteArrayOutputStream

/**
 * 24/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class YieldByteArrayOutputStream(size: Int, pop: (Array[Byte], Int, Boolean) => Unit) extends ByteArrayOutputStream(size) {

  override def write(b: Int): Unit = {
    if (count == size) {
      pop(buf, count, false)
      reset()
    }
    buf(count) = b.toByte
    count += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (count + len >= size) {
      pop(buf, count, false)
      reset()
    }
    System.arraycopy(b, off, buf, count, len)
    count += len
  }

  override def flush(): Unit = {
    pop(buf, count, false)
    reset()
  }

  override def close(): Unit = {
    pop(buf, count, true)
    reset()
  }

}
