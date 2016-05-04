package streaming.core.strategy.platform

import java.util.{List => JList, Map => JMap}

import serviceframework.dispatcher.Strategy


trait StreamingRuntime {


  def startRuntime: StreamingRuntime

  def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean = false): Boolean

  def streamingRuntimeInfo: StreamingRuntimeInfo

  def resetRuntimeOperator(runtimeOperator: RuntimeOperator)

  def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo)

  def awaitTermination

  def params:JMap[Any,Any]

}

trait StreamingRuntimeInfo

trait Event

case class JobFlowGenerate(jobName: String, index: Int, strategy: Strategy[Any]) extends Event

trait PlatformManagerListener {
  def processEvent(event: Event)
}
