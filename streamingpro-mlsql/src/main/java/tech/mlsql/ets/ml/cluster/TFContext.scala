package tech.mlsql.ets.ml.cluster

import net.sf.json.JSONObject
import tech.mlsql.common.utils.cluster.ml.{JobStatusRequest, MLWorkerProxy, WorkerInfo}
import tech.mlsql.common.utils.serder.json.JSONTool

/**
  * 2019-08-27 WilliamZhu(allwefantasy@gmail.com)
  */
class TFContext(val driverHost: DriverHost, val currentRole: CurrentRole) {
  val workerProxy = {
    new MLWorkerProxy(driverHost.host, driverHost.port) {
      override def workerTaskName(): WorkerInfo => String = {
        (f: WorkerInfo) => s"/job:worker/task:${f.taskIndex}"
      }

      override def parameterServerTaskName(): WorkerInfo => String = {
        (f: WorkerInfo) => s"/job:ps/task:${f.taskIndex}"
      }
    }
  }

  def close = {
    try {
      workerProxy.close
    } catch {
      case e: Exception =>
    }

  }

  /**
    * this is not a good way to kill python. We will figure out
    * more good way to solve this
    */
  def killPython(pro: Process) = {
    try {
      //kill -- -42
      val pid = getPidOfProcess(pro)
      if (pid == -1) {
        pro.destroyForcibly()
      } else {
        val subpid = os.proc("pgrep", "-P", pid).call(check = false).out.lines.head
        os.proc("kill", subpid).call(check = false)
      }
    } catch {
      case e: Exception =>
    }


  }

  def assertCommand = {
    require(!os.proc("pgrep").call(check = false).toString().contains("command not found"), "pgrep is required for this ET")
  }


  private def getPidOfProcess(p: Process): Int = {
    var pid = -1
    try {
      if (p.getClass.getName == "java.lang.UNIXProcess") {
        val f = p.getClass.getDeclaredField("pid")
        f.setAccessible(true)
        pid = f.getInt(p)
        f.setAccessible(false)
      }
    } catch {
      case e: Exception =>
        pid = -1
    }
    pid
  }

  def isPs = {
    currentRole.jobName == "ps"
  }

  def reportFails = {
    workerProxy.reportFails(JobStatusRequest(currentRole.jobName,
      currentRole.taskIndex, true, false, true))
  }

  def reportSuccess = {
    workerProxy.reportFails(JobStatusRequest(currentRole.jobName,
      currentRole.taskIndex, true, true, true))
  }

  def waitDoneOrFail = {
    val clusterSpec = workerProxy.fetchClusterSpec()
    workerProxy.waitDoneOrFail(clusterSpec.workers.size, JobStatusRequest(currentRole.jobName, currentRole.taskIndex, false, false, false))
  }

  def createRoleSpec = {
    val roleSpec = new JSONObject()
    roleSpec.put("jobName", currentRole.jobName)
    roleSpec.put("taskIndex", currentRole.taskIndex)
    roleSpec.toString()
  }

  def createClusterSpec = {
    val clusterSpec = workerProxy.fetchClusterSpec()
    val clusterSpecMap = Map("worker" -> clusterSpec.workers.map(f => s"${f.host}:${f.port}"),
      "ps" -> clusterSpec.parameterServers.map(f => s"${f.host}:${f.port}"))
    JSONTool.toJsonStr(clusterSpecMap)
  }
}

case class DriverHost(host: String, port: Int)

case class CurrentRole(jobName: String, taskIndex: Int)

