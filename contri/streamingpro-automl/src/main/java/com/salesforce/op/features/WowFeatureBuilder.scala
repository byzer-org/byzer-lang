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

package com.salesforce.op.features


import java.lang.reflect.Method

import com.salesforce.op.features.types._
import com.salesforce.op.RichFeatureSparkTypes._
import org.apache.spark.sql.{DataFrame, Row}
import com.salesforce.op.utils.spark.RichRow._
import streaming.common.ScalaObjectReflect

import scala.util.{Failure, Success, Try}
import scala.language.experimental.macros
import scala.reflect.runtime.universe._


/**
  * Created by allwefantasy on 19/9/2018.
  */
object WowFeatureBuilder {
  def fromDataFrame[ResponseType <: FeatureType : WeakTypeTag](
                                                                data: DataFrame,
                                                                response: String,
                                                                nonNullable: Set[String] = Set.empty,
                                                                feature2DataTypeMap: Map[String, String] = Map.empty
                                                              ): (Feature[ResponseType], Array[Feature[_ <: FeatureType]]) = {
    val allFeatures: Array[Feature[_ <: FeatureType]] =
      data.schema.fields.zipWithIndex.map { case (field, index) =>
        val isResponse = field.name == response
        val isNullable = !isResponse && !nonNullable.contains(field.name)
        val wtt: WeakTypeTag[_ <: FeatureType] = feature2DataTypeMap.get(field.name).map { clzzName =>
          FeatureSparkTypes.featureTypeFromString(clzzName)
        }.getOrElse(FeatureSparkTypes.featureTypeTagOf(field.dataType, isNullable))
        val feature = fromRow(name = field.name, index = Some(index), feature2DataTypeMap)(wtt)
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

  def invokeFeatureApply(clzzName: String, fieldValue: AnyRef) = {
    val (clzz, instance) = ScalaObjectReflect.findObjectMethod(clzzName)
    val methods = clzz.getDeclaredMethods.filter(f => f.getName == "apply")

    def convert = {
      instance match {
        case PickList => fieldValue.toString
        case _ => fieldValue
      }
    }

    methods.filter(f => f.getParameterTypes.head == classOf[Option[_]]).headOption.map { method =>
      method.invoke(instance, Option(convert))
    }.getOrElse {
      val method = methods.head
      method.invoke(instance, convert)
    }
  }

  def fromRow[O <: FeatureType : WeakTypeTag](name: String, index: Option[Int], feature2DataTypeMap: Map[String, String] = Map.empty): FeatureBuilderWithExtract[Row, O] = {
    val c = FeatureTypeSparkConverter[O]()
    new FeatureBuilderWithExtract[Row, O](
      name = name,
      extractFn = (r: Row) => {
        val fieldValue = index.map(r.get).getOrElse(r.getAny(name))
        val res = feature2DataTypeMap.get(name).map { clzzName =>
          invokeFeatureApply(clzzName, fieldValue.asInstanceOf[AnyRef]).asInstanceOf[O]
        }.getOrElse(c.fromSpark(fieldValue))
        res
      },
      extractSource =
        """
          |(r: Row) => {
          |        val fieldValue = index.map(r.get).getOrElse(r.getAny(name))
          |        feature2DataTypeMap.get(name).map { clzzName =>
          |          Class.forName(clzzName).getClass.getMethod("apply").invoke(null, fieldValue.asInstanceOf[AnyRef]).asInstanceOf[O]
          |        }.getOrElse(c.fromSpark(fieldValue))
          |      }
        """.stripMargin
    )
  }
}
