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

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import com.alibaba.sparkcube.CubeManager
import com.alibaba.sparkcube.catalog.{CubeHiveExternalCatalog, CubeInMemoryCatalog}
import com.alibaba.sparkcube.execution.api.SparkCubeSource


// sparkContext.ui is private to package spark
private[spark] class CubeSharedState(val session: SparkSession) extends Logging {

  logInfo("Initialize CubeSharedState")

  val cubeManager: CubeManager = new CubeManager()

  val cubeCatalog =
    session.sparkContext.getConf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => new CubeHiveExternalCatalog(session.sharedState.externalCatalog)
      case _ => new CubeInMemoryCatalog(session.sharedState.externalCatalog)
    }

}

object CubeSharedState {

  private val CUBE_STATE_LOCK = new Object()

  private val activeCubeSharedState: AtomicReference[CubeSharedState] =
    new AtomicReference[CubeSharedState](null)

  def get(session: SparkSession): CubeSharedState = CUBE_STATE_LOCK.synchronized {
    if (activeCubeSharedState.get() == null) {
      setActiveState(new CubeSharedState(session))
    }
    activeCubeSharedState.get
  }

  def setActiveState(
      state: CubeSharedState): Unit = CUBE_STATE_LOCK.synchronized {
    activeCubeSharedState.set(state)
  }
}
