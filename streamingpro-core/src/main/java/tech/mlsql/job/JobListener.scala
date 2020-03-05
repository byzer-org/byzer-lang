package tech.mlsql.job

abstract class JobListener {

  import JobListener._

  def onJobStarted(event: JobStartedEvent): Unit

  def onJobFinished(event: JobFinishedEvent): Unit

}

object JobListener {

  trait JobEvent

  class JobStartedEvent(val groupId:String) extends JobEvent

  class JobFinishedEvent(val groupId:String) extends JobEvent

}
