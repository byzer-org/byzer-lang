/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.util

import org.apache.spark.ml.linalg._
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.JavaConversions._

import org.apache.spark.sql.types.StructType

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

object MatrixSerDer {
  def serialize(matrix: Matrix): InternalRow = {
    new MatrixUDT().serialize(matrix)
  }

  def matrixSchema(): StructType = {
    new MatrixUDT().sqlType
  }

  def deserialize(b: Object): Matrix = {
    //    new MatrixUDT().deserialize(b)
    val row = b.asInstanceOf[Array[Object]]
    val tpe = row(0).asInstanceOf[Int].toByte
    val numRows = row(1).asInstanceOf[Int]
    val numCols = row(2).asInstanceOf[Int]
    val values = row(5).asInstanceOf[util.ArrayList[Double]].toIndexedSeq.toArray
    val isTransposed = row(6).asInstanceOf[Boolean]
    tpe match {
      case 0 =>
        val colPtrs = row(3).asInstanceOf[util.ArrayList[Int]].toIndexedSeq.toArray
        val rowIndices = row(4).asInstanceOf[util.ArrayList[Int]].toIndexedSeq.toArray
        new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
      case 1 =>
        new DenseMatrix(numRows, numCols, values, isTransposed)
    }
  }
}
