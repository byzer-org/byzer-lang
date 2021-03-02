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

package com.alibaba.sparkcube.optimizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SparkAgent, SparkSession}
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, ReCountDistinct, Sum}
import org.apache.spark.sql.types._

class GenPlanFromCacheSuite extends SparkFunSuite {

  test("test getDataType") {
    val op = GenPlanFromCache(SparkSession.builder().master("local").getOrCreate())
    assert(op.getDataType("count", IntegerType) == LongType)
    assert(op.getDataType("COUNT", StringType) == LongType)
    assert(op.getDataType("Count", DateType) == LongType)
    assert(op.getDataType("MIN", ShortType) == ShortType)
    assert(op.getDataType("Max", ByteType) == ByteType)
    assert(op.getDataType("pre_count_distinct", BooleanType) == BinaryType)
    assert(op.getDataType("AVG", LongType) == DoubleType)
    assert(op.getDataType("Sum", IntegerType) == LongType)
    assert(op.getDataType("Sum", ByteType) == LongType)
    assert(op.getDataType(
      "Sum", SparkAgent.createDecimal(10, 2)) == SparkAgent.createDecimal(20, 2))
    assert(op.getDataType("Sum", FloatType) == DoubleType)
  }

  test("test buildAggrFunc") {
    val op = GenPlanFromCache(SparkSession.builder().master("local").getOrCreate())
    val exp = BoundReference(0, IntegerType, nullable = true)
    assert(op.buildAggrFunc(Seq(exp), "Count", promo = true).isInstanceOf[Sum])
    assert(op.buildAggrFunc(Seq(exp), "count", promo = false).isInstanceOf[Count])
    assert(op.buildAggrFunc(Seq(exp), "SUM", promo = false).isInstanceOf[Sum])
    assert(op.buildAggrFunc(Seq(exp), "pre_count_distinct", promo = false).isInstanceOf[Count])
    assert(op.buildAggrFunc(
      Seq(exp), "pre_count_distinct", promo = true).isInstanceOf[ReCountDistinct])
  }

}
