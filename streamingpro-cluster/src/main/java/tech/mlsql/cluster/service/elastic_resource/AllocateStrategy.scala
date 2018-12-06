package tech.mlsql.cluster.service.elastic_resource

import streaming.log.Logging
import tech.mlsql.cluster.model.EcsResourcePool
import tech.mlsql.cluster.service.BackendService
import tech.mlsql.cluster.service.elastic_resource.local.LocalDeployInstance

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
  def plan(tags: Seq[String], allocateType: String): Option[BaseResource]

  def allocate(command: BaseResource, allocateType: String): Boolean
}

/*
    We will sample every 10 seconds in 5 minutes, if always busy, the we will allocate a new
    MLSQL instance. Then check again.
 */
class JobNumAwareAllocateStrategy extends AllocateStrategy with Logging {

  private val holder = new java.util.concurrent.ConcurrentHashMap[String, scala.collection.mutable.Queue[Int]]()

  override def allocate(command: BaseResource, allocateType: String): Boolean = {
    command match {
      case LocalResourceAllocation(tags) =>
        val tempResources = EcsResourcePool.items()

        if (tempResources.size() == 0) {
          logError("No resources available, Allocate new MLSQL instance fail")
          return false
        }
        val resource = tempResources.asScala.head
        return LocalDeployInstance.deploy(resource.id())


      case LocalResourceDeAllocation(tags) => throw new RuntimeException("not implemented yet")
      case ClusterResourceAllocation(tags) => throw new RuntimeException("not implemented yet")
      case ClusterResourceDeAllocation(tags) => throw new RuntimeException("not implemented yet")
    }
  }

  override def plan(tags: Seq[String], allocateType: String): Option[BaseResource] = {
    val tagsStr = tags.mkString(",")
    val backendsWithTags = BackendService.backendsWithTags(tagsStr).map(f => f.meta).toSet
    if (backendsWithTags.size == 0) return None
    val nonActiveBackend = BackendService.nonActiveBackend
    if (!holder.containsKey(tagsStr)) {
      holder.put(tagsStr, scala.collection.mutable.Queue[Int]())
    }
    val queue = holder.get(tagsStr)
    if ((backendsWithTags -- nonActiveBackend).size == 0) {
      queue.enqueue(0)
    } else {
      queue.enqueue(1)
    }

    if (queue.size > 30) {
      queue.dequeue()
      if (queue.sum == 0) {
        holder.remove(tagsStr)
        return allocateType match {
          case "local" => Option(LocalResourceAllocation(tagsStr))
          case "cluster" => Option(ClusterResourceAllocation(tagsStr))
        }
      }
      if (queue.sum > 15) {
        holder.remove(tagsStr)
        return allocateType match {
          case "local" => Option(LocalResourceDeAllocation(tagsStr))
          case "cluster" => Option(ClusterResourceDeAllocation(tagsStr))
        }
      }
    }
    return None
  }
}
