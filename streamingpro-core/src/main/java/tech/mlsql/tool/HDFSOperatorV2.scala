package tech.mlsql.tool

import java.io.{BufferedReader, ByteArrayOutputStream, File, InputStream, InputStreamReader}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.MLSQLSparkUtils
import org.apache.spark.deploy.SparkHadoopUtil
import tech.mlsql.common.utils.Md5

import scala.collection.mutable.ArrayBuffer

/**
 * 11/11/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object HDFSOperatorV2 {

  def hadoopConfiguration:Configuration = {
    if(MLSQLSparkUtils.sparkHadoopUtil != null){
      MLSQLSparkUtils.sparkHadoopUtil.conf
    } else new Configuration()

  }

  def readFile(path: String): String = {
    val fs = FileSystem.get(hadoopConfiguration)
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

  def getFileStatus(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    val file = fs.getFileStatus(new Path(path))
    file
  }


  def readAsInputStream(fileName: String): InputStream = {
    val fs = FileSystem.get(hadoopConfiguration)
    val src: Path = new Path(fileName)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
    } catch {
      case e: Exception =>
        if (in != null) in.close()
    }
    return in
  }


  def readBytes(fileName: String): Array[Byte] = {
    val fs = FileSystem.get(hadoopConfiguration)
    val src: Path = new Path(fileName)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
      val byteArrayOut = new ByteArrayOutputStream()
      IOUtils.copyBytes(in, byteArrayOut, 1024, true)
      byteArrayOut.toByteArray
    } finally {
      if (null != in) in.close()
    }
  }

  def listModelDirectory(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def listFiles(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.listStatus(new Path(path))
  }

  def saveBytesFile(path: String, fileName: String, bytes: Array[Byte]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(hadoopConfiguration)
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

  def saveStream(path: String, fileName: String, inputStream: InputStream) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(hadoopConfiguration)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      IOUtils.copyBytes(inputStream, dos, 4 * 1024 * 1024)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
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

  def ceateEmptyFile(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    val dos = fs.create(new Path(path))
    dos.close()
  }

  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(hadoopConfiguration)
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

  def getFilePath(path: String) = {
    new Path(path).toString
  }

  def copyToHDFS(tempLocalPath: String, path: String, cleanTarget: Boolean, cleanSource: Boolean) = {
    val fs = FileSystem.get(hadoopConfiguration)
    if (cleanTarget) {
      fs.delete(new Path(path), true)
    }
    fs.copyFromLocalFile(new Path(tempLocalPath),
      new Path(path))
    if (cleanSource) {
      FileUtils.forceDelete(new File(tempLocalPath))
    }

  }

  def copyToLocalFile(tempLocalPath: String, path: String, clean: Boolean) = {
    val fs = FileSystem.get(hadoopConfiguration)
    val tmpFile = new File(tempLocalPath)
    if (tmpFile.exists()) {
      FileUtils.forceDelete(tmpFile)
    }
    fs.copyToLocalFile(new Path(path), new Path(tempLocalPath))
  }

  def deleteDir(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.delete(new Path(path), true)
  }

  def isDir(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.isDirectory(new Path(path))
  }

  def isFile(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.isFile(new Path(path))
  }

  def fileExists(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.exists(new Path(path))
  }

  def createDir(path: String) = {
    val fs = FileSystem.get(hadoopConfiguration)
    fs.mkdirs(new Path(path))
  }

  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    val dir = "/tmp/train/" + Md5.md5Hash(path)
    if (autoCreateParentDir) {
      FileUtils.forceMkdir(new File(dir))
    }
    dir
  }

  def iteratorFiles(path: String, recursive: Boolean) = {
    val fs = FileSystem.get(hadoopConfiguration)
    val files = ArrayBuffer[String]()
    _iteratorFiles(fs, path, files)
    files
  }

  def _iteratorFiles(fs: FileSystem, path: String, files: ArrayBuffer[String]): Unit = {
    val p = new Path(path)
    val file = fs.getFileStatus(p)
    if (fs.exists(p)) {

      if (file.isFile) {
        files += p.toString
      }
      else if (file.isDirectory) {
        val fileStatusArr = fs.listStatus(p)
        if (fileStatusArr != null && fileStatusArr.length > 0) {
          for (tempFile <- fileStatusArr) {
            _iteratorFiles(fs, tempFile.getPath.toString, files)
          }
        }
      }
    }
  }
}
