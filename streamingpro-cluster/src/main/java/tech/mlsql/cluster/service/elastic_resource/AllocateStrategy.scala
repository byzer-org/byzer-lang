package tech.mlsql.cluster.service.elastic_resource

import tech.mlsql.cluster.model.EcsResourcePool
import tech.mlsql.cluster.service.BackendService


/**
  * 2018-12-05 WilliamZhu(allwefantasy@gmail.com)
  */

class BaseResource()

case class LocalResourceAllocation(tags: String, resource: Seq[EcsResourcePool]) extends BaseResource

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
  def allocate(tags: Seq[String], allocateType: String): Option[BaseResource]
}

/*
    We will sample every 30 seconds in 5 minutes, if always busy, the we will allocate a new
    MLSQL instance. Then check again.
 */
class JobNumAwareAllocateStrategy extends AllocateStrategy {
  private val holder = new java.util.concurrent.ConcurrentHashMap[String, scala.collection.mutable.Queue[Int]]()

  override def allocate(tags: Seq[String], allocateType: String): Option[BaseResource] = {
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

    if (queue.size > 10) {
      queue.dequeue()
      if (queue.sum == 0) {
        return allocateType match {
          case "local" =>
            
            Option(LocalResourceAllocation(tagsStr,Seq()))
          case "cluster" => Option(ClusterResourceAllocation(tagsStr))
        }
      }
      if (queue.sum > 5) {
        return allocateType match {
          case "local" => Option(LocalResourceDeAllocation(tagsStr))
          case "cluster" => Option(ClusterResourceDeAllocation(tagsStr))
        }
      }
    }
    return None
  }
}
