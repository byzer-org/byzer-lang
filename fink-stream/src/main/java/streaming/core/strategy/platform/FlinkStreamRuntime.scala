package streaming.core.strategy.platform

import java.util.{Map => JMap}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * Created by allwefantasy on 19/3/2017.
  */
class FlinkStreamRuntime(_params: JMap[Any, Any]) extends StreamingRuntime with PlatformManagerListener {
  self =>

  def name = "FlinkStream"

  def createRuntime = {
    StreamExecutionEnvironment.getExecutionEnvironment
  }

  override def startRuntime: StreamingRuntime = {
    this
  }

  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = ???

  override def streamingRuntimeInfo: StreamingRuntimeInfo = ???

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = ???

  override def configureStreamingRuntimeInfo(streamingRuntimeInfo: StreamingRuntimeInfo): Unit = ???

  override def awaitTermination: Unit = ???

  override def params: JMap[Any, Any] = ???

  override def processEvent(event: Event): Unit = ???
}
