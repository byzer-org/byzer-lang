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

package streaming.core.compositor.spark.udf

import java.util.UUID

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{DenseVector, Matrices, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.UDFRegistration
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import streaming.common.UnicodeUtils
import tech.mlsql.common.utils.base.Measurement
import tech.mlsql.common.utils.distribute.socket.server.ByteUnit

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray

/**
 * Created by allwefantasy on 3/5/2017.
 */
object Functions {
  def parse(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("parse", (co: String) => {
      val parseMethod = Class.forName("org.ansj.splitWord.analysis.NlpAnalysis").getMethod("parse", classOf[String])
      val tmp = parseMethod.invoke(null, co)
      val terms = tmp.getClass.getMethod("getTerms").invoke(tmp).asInstanceOf[java.util.List[Any]]
      terms.map(f => f.asInstanceOf[ {def getName: String}].getName).toArray
    })
  }

  def mkString(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("mkString", (sep: String, co: WrappedArray[String]) => {
      co.mkString(sep)
    })
  }

  def uuid(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("uuid", () => {
      UUID.randomUUID().toString.replace("-", "")
    })
  }

  def sleep(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("sleep", (sleep: Long) => {
      Thread.sleep(sleep)
      ""
    })
  }

  def vec_argmax(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_argmax", (vector: Vector) => {
      vector.argmax
    })
  }

  def vec_sqdist(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_sqdist", (vec1: Vector, vec2: Vector) => {
      Vectors.sqdist(vec1, vec2)
    })
  }

  def vec_norm(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_norm", (vec1: Vector, p: Double) => {
      Vectors.norm(vec1, p)
    })
  }

  def vec_dense(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_dense", (vec1: Seq[Double]) => {
      Vectors.dense(vec1.toArray)
    })
  }

  def vec_array(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_array", (vec1: Vector) => {
      vec1.toArray
    })
  }

  def vec_mk_string(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_mk_string", (splitter: String, vec1: Vector) => {
      vec1.toArray.mkString(splitter)
    })
  }

  def vec_sparse(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_sparse", (size: Int, vec1: Map[Int, Double]) => {
      Vectors.sparse(size, vec1.toSeq)
    })
  }

  def vec_concat(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_concat", (vecs: Seq[Vector]) => {
      FVectors.assemble(vecs: _*)
    })
  }

  def vec_slice(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_slice", (vec: Vector, inds: Seq[Int]) => {
      vec match {
        case features: DenseVector => Vectors.dense(inds.toArray.map(features.apply))
        case features: SparseVector => FVectors.slice(features, inds.toArray)
      }
    })
  }

  def vec_range(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_range", (vec: Vector, inds: Seq[Int]) => {
      assert(inds.size == 2)
      vec match {
        case features: DenseVector => Vectors.dense(features.toArray.slice(inds(0), inds(1)))
        case features: SparseVector => FVectors.range(features, inds.toArray)
      }
    })
  }


  /*
    1 - x.dot(y)/(x.norm(2)*y.norm(2))
   */
  def vec_cosine(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_cosine", (vec1: Vector, vec2: Vector) => {
      if ((vec1.size == 0 || vec2.size == 0 || vec1.size != vec2.size)) {
        0.0
      } else {
        val dot = new org.apache.spark.mllib.feature.ElementwiseProduct(OldVectors.fromML(vec1)).transform(OldVectors.fromML(vec2))
        var value = 0d
        val value_add = (a: Int, b: Double) => {
          value += b
        }
        dot.foreachActive(value_add)
        value / (Vectors.norm(vec1, 2) * Vectors.norm(vec2, 2))
      }
    })
  }

  def matrix_array(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("matrix_array", (matrix: Matrix) => {
      matrix.rowIter.map(_.toArray).toArray
    })
  }

  def vecWiseProduct(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_wise_mul", (vec1: Vector, vec2: Vector) => {
      (vec1.asBreeze *:* vec2.asBreeze).fromBreeze
    })
  }

  def vecWiseAdd(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_wise_add", (vec1: Vector, vec2: Vector) => {
      (vec1.asBreeze +:+ vec2.asBreeze).fromBreeze
    })
  }

  def vecWiseDifference(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_wise_dif", (vec1: Vector, vec2: Vector) => {
      (vec1.asBreeze -:- vec2.asBreeze).fromBreeze
    })
  }

  def vecWiseModulo(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_wise_mod", (vec1: Vector, vec2: Vector) => {
      (vec1.asBreeze %:% vec2.asBreeze).fromBreeze
    })
  }

  def vecWiseComparsion(uDFRegistration: UDFRegistration): Unit = {
    //    import BreezeImplicit._
    //    uDFRegistration.register("vec_wise_com", (vec1: Vector, vec2: Vector) => {
    //      (vec1.asBreeze <:< vec2.asBreeze)
    //    })
  }

  def vecInplaceAddition(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_inplace_add", (vec1: Vector, b: Double) => {
      (vec1.asBreeze :+= b).fromBreeze
    })
  }

  def vecInplaceElemWiseMul(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    uDFRegistration.register("vec_inplace_ew_mul", (vec1: Vector, b: Double) => {
      (vec1.asBreeze :*= b).fromBreeze
    })
  }

  def vecCeil(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    import breeze.numerics.ceil
    uDFRegistration.register("vec_ceil", (vec1: Vector) => {
      ceil(vec1.asBreeze).fromBreeze
    })
  }

  def vecMean(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    import breeze.stats.mean
    uDFRegistration.register("vec_mean", (vec1: Vector) => {
      mean(vec1.asBreeze)
    })
  }

  def vecStd(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    import breeze.stats.stddev
    uDFRegistration.register("vec_stddev", (vec1: Vector) => {
      stddev(vec1.asBreeze)
    })
  }

  // =================
  // matrix operation
  // =================

  def matrix(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("matrix_dense", (vectors: WrappedArray[WrappedArray[Double]]) => {
      require(vectors.size >= 1, "vectors length should >= 1.")
      require(vectors.map(_.length).toSet.size == 1, "require all array hash the same length.")
      val numRow = vectors.length
      val numCol = vectors.head.length
      var values = Array.empty[Double]

      for (i <- (0 until numCol)) {
        for (j <- (0 until numRow)) {
          values = values :+ vectors(j)(i)
        }
      }
      Matrices.dense(numRow, numCol, values)
    })
  }

  def matrixSum(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("matrix_sum", (matrix: Matrix, axis: Int) => {
      require(axis == 1 || axis == 0, s"axis $axis is out of bounds for matrix of dimension 2")
      import BreezeImplicit._
      var result: Vector = null
      if (axis == 0) {
        var vec: BV[Double] = null
        matrix.rowIter.foreach(v => {
          if (vec == null) {
            vec = v.asBreeze
          } else {
            vec = v.asBreeze + vec
          }
        })
        result = vec.fromBreeze
      } else {
        val values = matrix.rowIter.map(v => {
          v.toArray.sum
        })
        result = Vectors.dense(values.toArray)
      }
      result
    })
  }

  def vecFloor(uDFRegistration: UDFRegistration): Unit = {
    import BreezeImplicit._
    import breeze.numerics.floor
    uDFRegistration.register("vec_floor", (vec1: Vector) => {
      floor(vec1.asBreeze).fromBreeze
    })
  }

  def onehot(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("onehot", (a: Int, size: Int) => {
      val oneValue = Array(1.0)
      val emptyValues = Array.empty[Double]
      val emptyIndices = Array.empty[Int]
      if (a < size && a > 0) {
        Vectors.sparse(size, Array(a), oneValue)
      } else {
        Vectors.sparse(size, emptyIndices, emptyValues)
      }
    })
  }


  def array_intersect(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_intersect", (vec1: Seq[String], vec2: Seq[String]) => {
      vec1.intersect(vec2)
    })
  }


  def array_index(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_index", (vec1: Seq[String], word: Any) => {
      vec1.indexOf(word)
    })
  }

  def array_slice(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_slice", (vec1: Seq[String], from: Int, to: Int) => {
      if (to == -1) {
        vec1.slice(from, vec1.length)
      } else {
        vec1.slice(from, to)
      }
    })
  }

  def array_number_concat(uDFRegistration: UDFRegistration) = {

    uDFRegistration.register("array_number_concat", (a: Seq[Seq[Number]]) => {
      a.flatMap(f => f).map(f => f.doubleValue())
    })
  }

  def array_concat(uDFRegistration: UDFRegistration) = {

    uDFRegistration.register("array_concat", (a: Seq[Seq[String]]) => {
      a.flatMap(f => f)
    })
  }

  def array_number_to_string(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_number_to_string", (a: Seq[Number]) => {
      a.map(f => f.toString)
    })
  }

  def array_string_to_double(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_string_to_double", (a: Seq[String]) => {
      a.map(f => f.toDouble)
    })
  }

  def map_value_int_to_double(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("map_value_int_to_double", (a: Map[String, Int]) => {
      a.map(f => (f._1, f._2.toDouble))
    })
  }

  def array_string_to_float(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_string_to_float", (a: Seq[String]) => {
      a.map(f => f.toFloat)
    })
  }

  def array_string_to_int(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("array_string_to_int", (a: Seq[String]) => {
      a.map(f => f.toInt)
    })
  }

  def toArrayDouble(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("to_array_double", (seq: Seq[Object]) => {
      seq.map(a => a.toString.toDouble)
    })
  }

  def arrayOneHot(uDFRegistration: UDFRegistration) = {
    // TODO:(fchen)修改成稀疏矩阵的实现
    uDFRegistration.register("array_onehot", (a: WrappedArray[Int], size: Int) => {
      val vectors = a.map(toEncode => {
        val oneValue = Array(1.0)
        val emptyValues = Array.empty[Double]
        val emptyIndices = Array.empty[Int]
        if (toEncode < size) {
          Vectors.sparse(size, Array(toEncode), oneValue)
        } else {
          Vectors.sparse(size, emptyIndices, emptyValues)
        }
      })

      val values = (0 until vectors.head.size).asJava.map(colIndex => {
        (0 until a.size).map(rowIndex => {
          vectors(rowIndex).toArray(colIndex)
        })
      }).flatMap(x => x).toArray

      Matrices.dense(a.size, vectors.head.size, values)
    })
  }

  def ngram(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("ngram", (words: Seq[String], n: Int) => {
      words.iterator.sliding(n).withPartial(false).map(_.mkString(" ")).toSeq
    })
  }

  def decodeKafka(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("decodeKafka", (item: Array[Byte]) => {
      new String(item, "utf-8")
    })
  }


  def keepChinese(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("keepChinese", (item: String, keepPunctuation: Boolean, include: Seq[String]) => {
      UnicodeUtils.keepChinese(item, keepPunctuation, include.toArray)
    })
  }

  def paddingIntArray(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("padding_int_array", (seq: Seq[Int], length: Int, default: Int) => {
      if (seq.length > length) {
        seq.slice(0, length)
      } else {
        seq ++ Seq.fill(length - seq.length)(default)
      }
    })
  }


  // =================
  // unit operation
  // =================

  def byteStringAsBytes(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("byteStringAsBytes", (item: String) => {
      Measurement.byteStringAsBytes(item)
    })
  }

  def byteStringAsKb(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("byteStringAsKb", (item: String) => {
      Measurement.byteStringAsKb(item)
    })
  }

  def byteStringAsMb(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("byteStringAsMb", (item: String) => {
      Measurement.byteStringAsMb(item)
    })
  }

  def byteStringAsGb(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("byteStringAsGb", (item: String) => {
      Measurement.byteStringAsGb(item)
    })
  }

  def byteStringAs(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("byteStringAsGb", (item: String, unit: String) => {
      Measurement.byteStringAs(item, ByteUnit.valueOf(unit))
    })
  }


  // for spark 2.2.x
  object BreezeImplicit {

    implicit class AsBreeze(vector: Vector) {
      def asBreeze: BV[Double] = {
        vector match {
          case sv: SparseVector =>
            new BSV[Double](sv.indices, sv.values, sv.size)
          case dv: DenseVector =>
            new BDV[Double](dv.values)
          case v =>
            sys.error("unknow vector type.")
        }
      }
    }

    implicit class FromBreeze(breezeVector: BV[Double]) {
      def fromBreeze: Vector = {
        breezeVector match {
          case v: BDV[Double] =>
            if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
              new DenseVector(v.data)
            } else {
              new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
            }
          case v: BSV[Double] =>
            if (v.index.length == v.used) {
              new SparseVector(v.length, v.index, v.data)
            } else {
              new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
            }
          case v: BV[_] =>
            sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
        }
      }
    }

  }

  def parseDateAsLong(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("parseDateAsLong", (date: String, pattern: String) => {
      DateTime.parse(date, DateTimeFormat.forPattern(pattern)).getMillis
    })
  }

  def parseLongAsDate(uDFRegistration: UDFRegistration): Unit = {
    uDFRegistration.register("parseLongAsDate", (date: Long, pattern: String) => {
      val dt = new DateTime().withMillis(date)
      dt.toString(pattern)
    })
  }

  def timeAgo(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("timeAgo", (item: String) => {
      val seconds = Measurement.timeStringAsSec(item)
      System.currentTimeMillis() - 1000 * seconds
    })
  }

  def timePlus(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("timePlus", (time: Long, item: String) => {
      val seconds = Measurement.timeStringAsSec(item)
      time + 1000 * seconds
    })
  }

  def timeMinus(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("timeMinus", (time: Long, item: String) => {
      val seconds = Measurement.timeStringAsSec(item)
      time - 1000 * seconds
    })
  }


}
