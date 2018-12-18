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

package streaming.dsl.mmlib.algs.feature

import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql._

/**
  * Created by allwefantasy on 15/5/2018.
  */
object DiscretizerIntFeature extends BaseFeatureFunctions {
  def vectorize(df: DataFrame, mappingPath: String, fields: Seq[String]) = {

    // assemble double fields
    val _features_ = "_features_"

    val assembler = new VectorAssembler()
      .setInputCols(fields.toArray)
      .setOutputCol(_features_)

    var newDF = assembler.transform(df)

    val pca = new PCA()
      .setInputCol(_features_)
      .setOutputCol("_discretizerIntFeature_")
      .setK(3)
      .fit(newDF)
    pca.write.overwrite().save(mappingPath + "/discretizerIntFeature/pca")
    newDF = pca.transform(newDF)

    (fields ++ Seq("_features_")).foreach { f =>
      newDF = newDF.drop(f)
    }
    newDF
  }
}
