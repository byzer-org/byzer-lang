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

package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by allwefantasy on 7/8/2018.
  */
class WowFastRDD[T: ClassTag](sc: SparkContext,
                              rddName: String,
                              @transient private var data: Seq[T]
                             ) extends RDD[T](sc, Nil) {
  def this(sc: SparkContext, data: Seq[T]) = {
    this(sc, getClass.getSimpleName, data)
  }

  setName(rddName)

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    split.asInstanceOf[ParallelCollectionPartition[T]].iterator
  }

  def updateData(data: Seq[T]): Unit = {
    this.data = data
    this.markCheckpointed()
  }

  override protected def getPartitions: Array[Partition] = {
    Array(new ParallelCollectionPartition(id, 0, data))
  }
}
