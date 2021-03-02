/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.sparkcube.execution

import com.swoop.alchemy.spark.expressions.hll.HyperLogLogInitSimpleAgg

import org.apache.spark.sql.{SparkAgent, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{BitSetMapping, PreApproxCountDistinct, PreCountDistinct}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

case class PreCountDistinctTransformer(
    spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case agg@ Aggregate(_, _, child) =>
      val relatedFields = scala.collection.mutable.Queue[PreCountDistExprInfo]()
      agg.transformExpressions {
        case PreCountDistinct(childExpr) =>
          val deAttr = AttributeReference("dictionary_encoded_" + childExpr.prettyName, IntegerType,
            false)(NamedExpression.newExprId, Seq.empty[String])
          relatedFields += PreCountDistExprInfo(childExpr, deAttr)
          BitSetMapping(deAttr)
        case PreApproxCountDistinct(childExpr, relativeSD) =>
          HyperLogLogInitSimpleAgg(childExpr, relativeSD)
      }.withNewChildren {
        val dictionaries = relatedFields.map {
          case PreCountDistExprInfo(childExpr, encodedAttr) =>
            val (subPlan, attr) = CacheUtils
              .findAttributeRelationInPlan(childExpr, child)
              .getOrElse((child, childExpr))

            val windowSpec = Window.orderBy(SparkAgent.createColumn(attr))
            val exprName = childExpr match {
              case ne: NamedExpression => ne.name
              case _ => childExpr.prettyName
            }
            val dictPlan = GlobalDictionaryPlaceHolder(exprName, SparkAgent.getLogicalPlan(
              SparkAgent.getDataFrame(spark, subPlan).groupBy(SparkAgent.createColumn(attr)).agg(
                SparkAgent.createColumn(attr)).select(
                  SparkAgent.createColumn(attr) as ("dict_key"),
                  row_number().over(windowSpec) as "dict_value")))
            val key = dictPlan.output(0)
            val value = dictPlan.output(1)
            val valueAlias = Alias(value, encodedAttr.name)(encodedAttr.exprId)
            (Project(Seq(key, valueAlias), dictPlan), (childExpr, encodedAttr))
        }

        val result = dictionaries.foldLeft(child) {
          (joined, dict) =>
            val childExpr = dict._2._1
            val encodedAttr = dict._2._2
            val map = dict._1
            Project(joined.output :+ encodedAttr, Join(joined, map, Inner,
              Some(EqualTo(childExpr, map.projectList(0)))))
        }

        Seq(result)
      }
  }
}

case class RemoveGlobalDictionaryPlaceHolder(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GlobalDictionaryPlaceHolder(_, child) => child
  }
}

case class PreCountDistExprInfo(
    childExpr: Expression,
    encodedAttr: AttributeReference)
