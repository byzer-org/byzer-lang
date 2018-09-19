package com.salesforce.op.features


import com.salesforce.op.features.types._
import org.apache.spark.sql.{DataFrame, Row}
import com.salesforce.op.utils.spark.RichRow._

import scala.language.experimental.macros
import scala.reflect.runtime.universe._

/**
  * Created by allwefantasy on 19/9/2018.
  */
object WowFeatureBuilder {
  def fromDataFrame[ResponseType <: FeatureType : WeakTypeTag](
                                                                data: DataFrame,
                                                                response: String,
                                                                nonNullable: Set[String] = Set.empty
                                                              ): (Feature[ResponseType], Array[Feature[_ <: FeatureType]]) = {
    val allFeatures: Array[Feature[_ <: FeatureType]] =
      data.schema.fields.zipWithIndex.map { case (field, index) =>
        val isResponse = field.name == response
        val isNullable = !isResponse && !nonNullable.contains(field.name)
        val wtt: WeakTypeTag[_ <: FeatureType] = FeatureSparkTypes.featureTypeTagOf(field.dataType, isNullable)
        val feature = fromRow(name = field.name, index = Some(index))(wtt)
        if (isResponse) feature.asResponse else feature.asPredictor
      }
    val (responses, features) = allFeatures.partition(_.name == response)
    val responseFeature = responses.toList match {
      case feature :: Nil if feature.isSubtypeOf[ResponseType] =>
        feature.asInstanceOf[Feature[ResponseType]]
      case feature :: Nil =>
        throw new RuntimeException(
          s"Response feature '$response' is of type ${feature.typeName}, but expected ${FeatureType.typeName[ResponseType]}")
      case Nil =>
        throw new RuntimeException(s"Response feature '$response' was not found in dataframe schema")
      case _ =>
        throw new RuntimeException(s"Multiple features with name '$response' were found (should not happen): "
          + responses.map(_.name).mkString(","))
    }
    responseFeature -> features
  }

  def fromRow[O <: FeatureType : WeakTypeTag](implicit name: sourcecode.Name): FeatureBuilderWithExtract[Row, O] = fromRow[O](name.value, None)

  def fromRow[O <: FeatureType : WeakTypeTag](name: String): FeatureBuilderWithExtract[Row, O] = fromRow[O](name, None)

  def fromRow[O <: FeatureType : WeakTypeTag](index: Int)(implicit name: sourcecode.Name): FeatureBuilderWithExtract[Row, O] = fromRow[O](name.value, Some(index))

  def fromRow[O <: FeatureType : WeakTypeTag](name: String, index: Option[Int]): FeatureBuilderWithExtract[Row, O] = {
    val c = FeatureTypeSparkConverter[O]()
    new FeatureBuilderWithExtract[Row, O](
      name = name,
      extractFn = (r: Row) => {
        c.fromSpark(index.map(r.get).getOrElse(r.getAny(name)))
      },
      extractSource = "(r: Row) => c.fromSpark(index.map(r.get).getOrElse(r.getAny(name)))"
    )
  }
}
