package org.apache.spark.util

import java.util

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, VectorUDT}
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 6/2/2018.
  */
object VectorSerDer {

  def ser_vector(vec: org.apache.spark.ml.linalg.Vector) = {
    new VectorUDT().serialize(vec)
  }

  def vector_schema() = {
    new VectorUDT().sqlType
  }

  def deser_vector(b: Object) = {
    val row = b.asInstanceOf[Array[Object]]
    val tpe = row(0).asInstanceOf[Int].toByte
    tpe match {
      case 0 =>
        val size = row(1).asInstanceOf[Int]
        val indices = row(2).asInstanceOf[util.ArrayList[Int]].toIndexedSeq.toArray
        val values = row(3).asInstanceOf[util.ArrayList[Double]].toIndexedSeq.toArray
        new SparseVector(size, indices, values)
      case 1 =>
        val values = row(3).asInstanceOf[util.ArrayList[Double]].toIndexedSeq.toArray
        new DenseVector(values)

    }
  }
}