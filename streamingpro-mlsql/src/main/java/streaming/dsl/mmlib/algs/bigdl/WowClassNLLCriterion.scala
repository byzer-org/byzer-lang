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

package streaming.dsl.mmlib.algs.bigdl

import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric


object WowClassNLLCriterion {
  def apply(
             paramsExtractor: ClassWeightParamExtractor
           )(implicit ev: TensorNumeric[Float]): ClassNLLCriterion[Float] = {
    val weights = paramsExtractor.weights.map(f => Tensor(f, Array(f.size))).getOrElse(null)
    new ClassNLLCriterion[Float](weights,
      paramsExtractor.sizeAverage.getOrElse(true),
      paramsExtractor.logProbAsInput.getOrElse(true),
      paramsExtractor.paddingValue.getOrElse(-1)
    )
  }
}
