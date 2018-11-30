package streaming.dsl.mmlib.algs

import java.util.concurrent.ConcurrentHashMap

import com.salesforce.op.WowOpWorkflow
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 20/9/2018.
  */
object WorkflowManager {
  private val workflows = new ConcurrentHashMap[String, OpWorkflowInfo]()

  def get(name: String) = {
    workflows.get(name)
  }

  def put(name: String, opWorkflowInfo: OpWorkflowInfo) = {
    workflows.put(name, opWorkflowInfo)
  }

  def items = {
    workflows.values().toSeq
  }
}

case class OpWorkflowInfo(name: String, wowOpWorkflow: WowOpWorkflow)
