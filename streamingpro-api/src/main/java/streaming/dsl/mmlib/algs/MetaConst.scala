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

package streaming.dsl.mmlib.algs

/**
  * Created by allwefantasy on 22/5/2018.
  */
object MetaConst {
  def TF_IDF_PATH(path: String, col: String) = s"$path/columns/$col/tfidf"

  def WORD2VEC_PATH(path: String, col: String) = s"$path/columns/$col/word2vec"

  def WORD_INDEX_PATH(path: String, col: String) = s"$path/columns/$col/wordIndex"

  def ANALYSYS_WORDS_PATH(path: String, col: String) = s"$path/columns/$col/analysiswords"

  def OUTLIER_VALUE_PATH(path: String, col: String) = s"$path/columns/$col/outlierValues"

  def MIN_MAX_PATH(path: String, col: String) = s"$path/columns/$col/minMax"

  def STANDARD_SCALER_PATH(path: String, col: String) = s"$path/columns/$col/standardScaler"

  def DISCRETIZER_PATH(path: String) = s"$path/columns/discretizer"

  def QUANTILE_DISCRETIZAR_PATH(path: String, col: String) = s"$path/columns/$col/quantileDiscretizer"

  def PARAMS_PATH(path: String, col: String) = s"$path/params"

  def WORDS_PATH(path: String) = s"$path/words"

  def getDataPath(path: String) = {
    s"${path.stripSuffix("/")}/data"
  }

  def getMetaPath(path: String) = {
    s"${path.stripSuffix("/")}/meta"
  }
}
