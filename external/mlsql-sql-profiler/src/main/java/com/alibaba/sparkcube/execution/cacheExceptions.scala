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

import org.apache.spark.sql.AnalysisException

import com.alibaba.sparkcube.optimizer.CacheIdentifier

class CacheNotExistException(cacheId: CacheIdentifier)
  extends AnalysisException(s"Cube Management['${cacheId.cacheName}'] is not cached in" +
    s" '${cacheId.db}'.'${cacheId.viewName}'")

class CacheIsBuildingException(cacheId: CacheIdentifier)
  extends AnalysisException(s"Other is building Cube Management['${cacheId.cacheName}'] of" +
    s" '${cacheId.db}'.'${cacheId.viewName}' now, do not allow concurrent cache building.")

class NotCachedException(db: String, view: String)
  extends AnalysisException(s"'$db'.'$view' is not cached.")

class SparkCubeException(cacheId: CacheIdentifier, errorMsg: String)
  extends AnalysisException(s"Error on Cube Management ${cacheId.cacheName} of" +
    s" ${cacheId.db}.${cacheId.viewName}, $errorMsg")
