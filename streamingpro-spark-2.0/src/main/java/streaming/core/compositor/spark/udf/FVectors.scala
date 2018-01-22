//package streaming.core.compositor.spark.udf
//
//import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
//
///**
//  * Created by allwefantasy on 22/1/2018.
//  */
//object FVectors {
//  def dot(v1: Vector, v2: Vector): Double = {
//    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
//      s"=${v2.size}.")
//    var squaredDistance = 0.0
//    (v1, v2) match {
//      case (v1: SparseVector, v2: SparseVector) =>
//        val v1Values = v1.values
//        val v1Indices = v1.indices
//        val v2Values = v2.values
//        val v2Indices = v2.indices
//        val nnzv1 = v1Indices.length
//        val nnzv2 = v2Indices.length
//
//        var kv1 = 0
//        var kv2 = 0
//        while (kv1 < nnzv1 || kv2 < nnzv2) {
//          var score = 0.0
//
//          if (kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
//            score = v1Values(kv1)
//            kv1 += 1
//          } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))) {
//            score = v2Values(kv2)
//            kv2 += 1
//          } else {
//            score = v1Values(kv1) - v2Values(kv2)
//            kv1 += 1
//            kv2 += 1
//          }
//          squaredDistance += score * score
//        }
//
//      case (v1: SparseVector, v2: DenseVector) =>
//        squaredDistance = sqdist(v1, v2)
//
//      case (v1: DenseVector, v2: SparseVector) =>
//        squaredDistance = sqdist(v2, v1)
//
//      case (DenseVector(vv1), DenseVector(vv2)) =>
//        var kv = 0
//        val sz = vv1.length
//        while (kv < sz) {
//          val score = vv1(kv) - vv2(kv)
//          squaredDistance += score * score
//          kv += 1
//        }
//      case _ =>
//        throw new IllegalArgumentException("Do not support vector type " + v1.getClass +
//          " and " + v2.getClass)
//    }
//    squaredDistance
//  }
//}
