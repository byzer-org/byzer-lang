package streaming.common.hdfs.lock

import java.util.UUID
import java.util.concurrent.TimeoutException

import streaming.common.HDFSOperator

/**
  * 2019-03-07 WilliamZhu(allwefantasy@gmail.com)
  */
class DistrLocker(_dir: String) {

  def dir = {
    if (_dir.endsWith("/")) _dir.substring(0, _dir.length - 2)
    else _dir
  }

  val lock_id = "name-" + UUID.randomUUID().toString + ".lock"

  def createLock = {
    HDFSOperator.ceateEmptyFile(dir + "/" + lock_id)
  }

  def fetchLock() = {

    def _fetchLockFile = {
      val file = HDFSOperator.listFiles(dir).filter(f => f.getPath.getName.endsWith(".lock") && f.getPath.getName.startsWith("name-")).map { fileStatus =>
        val time = fileStatus.getModificationTime
        (time, fileStatus.getPath.getName)
      }.sortBy(f => f._1).last._2
      file
    }


    if (_fetchLockFile == lock_id) {
      Thread.sleep(2000)
      _fetchLockFile == lock_id
    } else false
  }

  def waitOtherLockToRelease(timeout: Long) = {

    def _fetchLocks = {
      HDFSOperator.listFiles(dir).filter(f => f.getPath.getName.endsWith(".lock") && f.getPath.getName.startsWith("name-"))
    }

    var count = 0
    while (_fetchLocks.size > 0 && count < timeout) {
      Thread.sleep(10000)
      count += 10
    }
    if (count >= timeout) {
      throw new TimeoutException(s"The dir ${dir} is locked time > $timeout")
    }


  }

  def releaseLock(): Unit = {
    HDFSOperator.deleteDir(dir + "/" + lock_id)
  }

}
