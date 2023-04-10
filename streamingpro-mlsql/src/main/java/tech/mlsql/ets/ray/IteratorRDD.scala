package tech.mlsql.ets.ray

import org.apache.spark.rdd.RDD

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import java.io.Serializable
import scala.reflect.ClassTag

class IteratorPartition[T: ClassTag](
                                      var rddId: Long,
                                      var slice: Int,
                                      var values: Iterator[T]
                                    ) extends Partition with Serializable {
  def iterator: Iterator[T] = values

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: IteratorPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice
}

class IteratorRDD[T: ClassTag](sc: SparkContext, @transient data: Iterator[T]) extends RDD[T](sc, Nil) {
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[IteratorPartition[T]].iterator)
  }

  override def getPartitions: Array[Partition] = Array(new IteratorPartition(id, 0, data))
}
