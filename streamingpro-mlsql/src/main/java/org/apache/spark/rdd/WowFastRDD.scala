package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by allwefantasy on 7/8/2018.
  */
class WowFastRDD[T: ClassTag](sc: SparkContext,
                              rddName: String,
                              @transient private var data: Seq[T]
                             ) extends RDD[T](sc, Nil) {
  def this(sc: SparkContext, data: Seq[T]) = {
    this(sc, getClass.getSimpleName, data)
  }

  setName(rddName)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

  def updateData(data: Seq[T]): Unit = {
    this.data = data
    this.markCheckpointed()
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new ParallelCollectionPartition(id, 0, data))
  }
}
