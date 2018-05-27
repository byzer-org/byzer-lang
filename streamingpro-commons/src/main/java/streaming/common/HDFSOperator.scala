package streaming.common

import java.io.{BufferedReader, File, InputStreamReader}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileStatus, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * 5/5/16 WilliamZhu(allwefantasy@gmail.com)
  */
object HDFSOperator {

  def readFile(path: String): String = {
    val fs = FileSystem.get(new Configuration())
    var br: BufferedReader = null
    var line: String = null
    val result = new ArrayBuffer[String]()
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      line = br.readLine()
      while (line != null) {
        result += line
        line = br.readLine()
      }
    } finally {
      if (br != null) br.close()
    }
    result.mkString("\n")

  }

  def listModelDirectory(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def saveBytesFile(path: String, fileName: String, bytes: Array[Byte]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      dos.write(bytes)
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }


  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(path + s"/$fileName"), true)
      iterator.foreach { x =>
        dos.writeBytes(x._2 + "\n")
      }
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }

  def copyToHDFS(tempModelLocalPath: String, path: String, clean: Boolean) = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
    fs.copyFromLocalFile(new Path(tempModelLocalPath),
      new Path(path))
    FileUtils.forceDelete(new File(tempModelLocalPath))
  }

  def copyToLocalFile(tempModelLocalPath: String, path: String, clean: Boolean) = {
    val fs = FileSystem.get(new Configuration())
    FileUtils.forceDelete(new File(tempModelLocalPath))
    fs.copyToLocalFile(new Path(path), new Path(tempModelLocalPath))
  }

  def deleteDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }

  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    val dir = "/tmp/train/" + Md5.md5Hash(path)
    if (autoCreateParentDir) {
      FileUtils.forceMkdir(new File(dir))
    }
    dir
  }


  def main(args: Array[String]): Unit = {
    println(readFile("file:///Users/allwefantasy/streamingpro/flink.json"))
  }
}
