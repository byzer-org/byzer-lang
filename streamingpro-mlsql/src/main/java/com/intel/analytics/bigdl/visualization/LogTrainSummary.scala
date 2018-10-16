package com.intel.analytics.bigdl.visualization

import streaming.log.{Logging, WowLog}

class LogTrainSummary(logDir: String,
                      appName: String) extends TrainSummary(logDir, appName) with Logging with WowLog {

  override def addScalar(tag: String, value: Float, step: Long): LogTrainSummary.this.type = {
    //    tag match {
    //      case "Throughput" =>
    //        logInfo(format(s"global step: ${step}  Throughput is ${value} records/second. "))
    //      case "Loss" =>
    //        logInfo(format(s"global step: ${step}  Loss is ${value}"))
    //      case _ =>
    //        logInfo(format(s"global step: ${step}  ${tag} is ${value}"))
    //    }

    super.addScalar(tag, value, step)
  }
}

class LogValidateSummary(logDir: String,
                         appName: String) extends ValidationSummary(logDir, appName) with Logging with WowLog {
  override def addScalar(tag: String, value: Float, step: Long): LogValidateSummary.this.type = {
    //logInfo(format(s"global step: ${step}  ${tag} is ${value}"))
    super.addScalar(tag, value, step)
  }
}
