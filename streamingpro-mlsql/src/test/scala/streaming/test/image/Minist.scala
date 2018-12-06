package streaming.test.image

import java.io.File

import org.apache.commons.io.FileUtils
import streaming.common.shell.ShellCommand
import streaming.log.Logging

/**
  * 2018-11-29 WilliamZhu(allwefantasy@gmail.com)
  */
object Minist extends Logging {
  val trainImages = "train-images-idx3-ubyte.gz"
  val trainLabels = "train-labels-idx1-ubyte.gz"
  val testImages = "t10k-images-idx3-ubyte.gz"
  val testLabels = "t10k-labels-idx1-ubyte.gz"

  def download(fileName: String, path: String) = {
    val url = s"http://yann.lecun.com/exdb/mnist/${fileName}"
    var res = ShellCommand.exec(s"cd $path;wget $url")
    logInfo(res)
    res = ShellCommand.exec(s"cd $path; gzip -d $fileName")
    logInfo(res)
  }

  def downloadMnist() = {
    val path = s"""/tmp/mlsql-minist-data"""
    val ministFile = new File(path)
    if (!ministFile.exists() || ministFile.listFiles().size < 4) {
      FileUtils.deleteDirectory(ministFile)
      ministFile.mkdirs()
      logInfo("downloading mnist data....")
      download(trainImages, path)
      download(trainLabels, path)
      download(testImages, path)
      download(testLabels, path)
      logInfo("mnist data is downloaded, the path is " + path)
    } else {
      logInfo("mnist data is already exists in  " + path + ", no need to download again")
    }

    path
  }
}
