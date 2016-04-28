package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ForEachDStream}
import org.apache.spark.util.Utils

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.reflect.ClassTag

/**
 * 4/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
class TestOutputStream[T: ClassTag](
                                     parent: DStream[T],
                                     val output: SynchronizedBuffer[Seq[T]] =
                                     new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]]
                                     ) extends ForEachDStream[T](parent, (rdd: RDD[T], t: Time) => {
  val collected = rdd.collect()
  output += collected
}) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    output.clear()
  }


  def registerMe() = {
    ssc.graph.addOutputStream(this)
  }

}
