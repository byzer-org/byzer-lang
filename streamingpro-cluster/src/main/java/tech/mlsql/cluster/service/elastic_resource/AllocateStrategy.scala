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

package tech.mlsql.cluster.service.elastic_resource

import tech.mlsql.cluster.model.{Backend, EcsResourcePool, ElasticMonitor}
import tech.mlsql.cluster.service.BackendService
import tech.mlsql.cluster.service.elastic_resource.local.LocalDeployInstance
import tech.mlsql.common.utils.log.Logging

import scala.collection.JavaConverters._


/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */

class BaseResource()

case class LocalResourceAllocation(tags: String) extends BaseResource

case class LocalResourceDeAllocation(tags: String) extends BaseResource

case class ClusterResourceAllocation(tags: String) extends BaseResource

case class ClusterResourceDeAllocation(tags: String) extends BaseResource

trait AllocateStrategy {
  /*
     check the mlsql instances which have specific tags are enough.
     If the size of return value is zero, means nothing should do.
     If the value is LocalResourceAllocation, then deploy new instances.
     If the value is LocalResourceDeAllocation, then shutdown some instances
   */
  def plan(tags: Seq[String], em: ElasticMonitor): Option[BaseResource]

  def allocate(command: BaseResource, em: ElasticMonitor): Boolean
}

/*
    We will sample every 10 seconds in 5 minutes, if always busy, the we will allocate a new
    MLSQL instance. Then check again.
 */
class JobNumAwareAllocateStrategy extends AllocateStrategy with Logging {

  var rounds = 30

  // these methods start with "fetch" should be overwrite if needs, cause they depends database
  // and this will make it's difficult to test our strategy. please
  // check how we do in `tech.mlsql.cluster.test.DeploySpec`
  def fetchAvailableResources = {
    EcsResourcePool.items()
  }

  def fetchBackendsWithTags(tagsStr: String) = {
    BackendService.backendsWithTags(tagsStr).map(f => f.meta).toSet
  }

  def fetchAllActiveBackends() = {
    BackendService.activeBackend.map(f => f._1).toSet
  }

  def fetchAllNonActiveBackendsWithTags(tagsStr: String) = {
    BackendService.nonActiveBackend.filter(f => tagsStr.split(",").toSet.intersect(f.getTags.toSet).size > 0)
  }

  //only for testing
  def ecsResourcePoolForTesting(backend: Backend) = {
    EcsResourcePool.findById(backend.getEcsResourcePoolId)
  }

  def dryRun() = {
    false
  }

  private val holder = new java.util.concurrent.ConcurrentHashMap[String, scala.collection.mutable.Queue[Int]]()

  override def allocate(command: BaseResource, em: ElasticMonitor): Boolean = {
    command match {
      case LocalResourceAllocation(tags) =>
        val tempResources = fetchAvailableResources

        if (tempResources.size() == 0) {
          logError(s"No resources available for ${tags}, Allocate new MLSQL instance fail")
          return false
        }
        val resource = tempResources.asScala.head
        if (dryRun()) {
          return LocalDeployInstance._deploy(resource, dryRun())
        } else {
          return LocalDeployInstance.deploy(resource.id())
        }

      case LocalResourceDeAllocation(tags) => {
        var success = false
        fetchAllNonActiveBackendsWithTags(tags).headOption match {
          case Some(backendWillBeRemove) =>
            success = if (dryRun()) {
              LocalDeployInstance._unDeploy(backendWillBeRemove, ecsResourcePoolForTesting(backendWillBeRemove), dryRun())
            } else {
              LocalDeployInstance.unDeploy(backendWillBeRemove.id())
            }

          case None =>
        }
        success
      }
      case ClusterResourceAllocation(tags) => throw new RuntimeException("not implemented yet")
      case ClusterResourceDeAllocation(tags) => throw new RuntimeException("not implemented yet")
    }
  }

  override def plan(tags: Seq[String], em: ElasticMonitor): Option[BaseResource] = {
    val tagsStr = tags.mkString(",")
    val allocateType = em.getAllocateType
    val backendsWithTags = fetchBackendsWithTags(tagsStr)
    if (backendsWithTags.size == 0) {
      logError("the number of instances tagged with ${tagsStr} is zero now, JobNumAwareAllocateStrategy" +
        "do not support this type.")
      return None
    }
    if (backendsWithTags.size == em.getMaxInstances) {
      logInfo(s"the number of instances tagged with ${tagsStr} have already retched max size [${em.getMaxInstances}]")
      return None
    }

    if (backendsWithTags.size < em.getMinInstances) {
      logInfo(s"the number of instances tagged with ${tagsStr} have already lower min size [${em.getMinInstances}]")
      return allocateType match {
        case "local" => Option(LocalResourceAllocation(tagsStr))
        case "cluster" => Option(ClusterResourceAllocation(tagsStr))
      }
    }

    val activeBackends = fetchAllActiveBackends
    if (!holder.containsKey(tagsStr)) {
      holder.put(tagsStr, scala.collection.mutable.Queue[Int]())
    }
    val queue = holder.get(tagsStr)
    logDebug(s"backendsWithTags:${backendsWithTags.size} nonActiveBackend:${activeBackends.size} ${(backendsWithTags -- activeBackends).size}")
    // all busy
    if ((backendsWithTags -- activeBackends).size == 0) {
      queue.enqueue(0)
    } else {
      queue.enqueue(1)
    }

    if (queue.size > rounds) {
      queue.dequeue()
      if (queue.sum == 0) {
        holder.remove(tagsStr)
        logDebug(s"check ${rounds} times, instances with ${tagsStr} always are busy, try to allocate new instance")
        return allocateType match {
          case "local" => Option(LocalResourceAllocation(tagsStr))
          case "cluster" => Option(ClusterResourceAllocation(tagsStr))
        }
      }
      if (queue.sum > rounds / 2) {
        holder.remove(tagsStr)
        logDebug(s"check ${rounds} times, instances with ${tagsStr} always are idle, try to reduce the number of instances")
        return allocateType match {
          case "local" => Option(LocalResourceDeAllocation(tagsStr))
          case "cluster" => Option(ClusterResourceDeAllocation(tagsStr))
        }
      }
    }
    return None
  }
}
