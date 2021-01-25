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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{BoundReference, SpecificInternalRow}
import org.apache.spark.sql.types.IntegerType

class CacheFunctionsSuite extends SparkFunSuite {

  def arrayEquals(op1: Any, op2: Any): Boolean = {
    if (op1.isInstanceOf[Array[Byte]] && op2.isInstanceOf[Array[Byte]]) {
      val array1 = op1.asInstanceOf[Array[Byte]]
      val array2 = op2.asInstanceOf[Array[Byte]]
      if (array1.length == array2.length) {
        return array1.zip(array2).forall(p => p._1 == p._2)
      }
    }
    false
  }

  test("test BitSetMapping") {
    val bsm1 = BitSetMapping(new BoundReference(0, IntegerType, true))
    val buffer1 = bsm1.createAggregationBuffer()
    val input1 = new SpecificInternalRow(Seq(IntegerType))
    input1.setInt(0, 1)
    bsm1.update(buffer1, input1)

    val bsm2 = BitSetMapping(new BoundReference(0, IntegerType, true))
    val buffer2 = bsm2.createAggregationBuffer()
    bsm2.update(buffer2, input1)
    bsm2.update(buffer2, input1)

    assert(arrayEquals(bsm1.eval(buffer1), bsm2.eval(buffer2)))

    val input2 = new SpecificInternalRow(Seq(IntegerType))
    input2.setInt(0, 2)
    bsm2.update(buffer2, input2)
    assert(!arrayEquals(bsm1.eval(buffer1), bsm2.eval(buffer2)))
  }
}
