package org.apache.spark.mllib.feature

import java.lang.{Iterable => JavaIterable}

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by allwefantasy on 18/1/2018.
  */
class IntTF(val numFeatures: Int) extends Serializable {

  def this() = this(1 << 20)

  var binary = false

  def setBinary(value: Boolean): this.type = {
    binary = value
    this
  }


  def transform(document: Iterable[Int]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
    document.foreach { term =>
      val i = term
      termFrequencies.put(i, setTF(i))
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /**
    * Transforms the input document into a sparse term frequency vector (Java version).
    */
  @Since("1.1.0")
  def transform(document: JavaIterable[Int]): Vector = {
    transform(document.asScala)
  }

}


