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

import org.apache.spark.sql.catalyst.catalog.BucketSpec

sealed trait CacheSchema

case class CacheRawSchema(cols: Seq[String] = Seq("*")) extends CacheSchema

case class CacheCubeSchema(dims: Seq[String], measures: Seq[Measure]) extends CacheSchema

case class Measure(column: String, func: String) {

  def name: String = column + "_" + func.toLowerCase

  override def toString: String = s"$func($column)"
}

object Measure {
  def apply(str: String): Measure = {
    val index = str.indexOf("(")
    val func = str.substring(0, index)
    val column = str.substring(index + 1, str.length - 1)
    Measure(column, func)
  }
}

case class CacheFormatInfo(
                            cacheName: String,
                            rewriteEnabled: Boolean = true,
                            partitionColumns: Option[Seq[String]] = None,
                            zorderColumns: Option[Seq[String]] = None,
                            provider: String = "PARQUET",
                            bucketSpec: Option[BucketSpec] = None,
                            cacheSchema: CacheSchema = CacheRawSchema())

case class CacheIdentifier(
                            db: Option[String],
                            viewName: String,
                            cacheName: String) extends java.lang.Comparable[CacheIdentifier] {

  override def compareTo(o: CacheIdentifier): Int = {
    return hashCode - o.hashCode
  }

  override def hashCode: Int = {
    var h = 17
    h = h * 37 + db.hashCode()
    h = h * 37 + viewName.hashCode()
    h = h * 37 + cacheName.hashCode()
    h
  }

  override def equals(other: Any): Boolean = other match {
    case otherId: CacheIdentifier =>
      db == otherId.db && viewName == otherId.viewName && cacheName == otherId.cacheName
    case _ =>
      false
  }

  override def toString: String = {
    s"$db.$viewName.$cacheName"
  }
}

object CacheIdentifier {
  def apply(str: String): CacheIdentifier = {
    val splits = str.split("\\.")
    splits match {
      case Array(db, table, view) => CacheIdentifier(Some(db), table, view)
      case Array(table, view) => CacheIdentifier(None, table, view)
      case _ => throw new RuntimeException(s"Invalid cache identifier[$str].")
    }

  }
}
