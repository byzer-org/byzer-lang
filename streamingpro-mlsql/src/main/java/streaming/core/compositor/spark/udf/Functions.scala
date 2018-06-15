package streaming.core.compositor.spark.udf

import scala.collection.mutable.WrappedArray
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{DenseVector, Matrices, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.UDFRegistration
import streaming.common.UnicodeUtils

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

  /*
    1 - x.dot(y)/(x.norm(2)*y.norm(2))
   */
  def vec_cosine(uDFRegistration: UDFRegistration) = {
    uDFRegistration.register("vec_cosine", (vec1: Vector, vec2: Vector) => {
      val dot = new org.apache.spark.mllib.feature.ElementwiseProduct(OldVectors.fromML(vec1)).transform(OldVectors.fromML(vec2))
      var value = 0d
      val value_add = (a: Int, b: Double) => {
        value += b
      }
      dot.foreachActive(value_add)
      value / (Vectors.norm(vec1, 2) * Vectors.norm(vec2, 2))
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
      if (a < size) {
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

}
