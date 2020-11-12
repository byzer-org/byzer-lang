package tech.mlsql.tool

import java.io.{BufferedReader, ByteArrayOutputStream, File, InputStream, InputStreamReader}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.Md5

import scala.collection.mutable.ArrayBuffer

/**
 * 11/11/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object HDFSOperatorV2 {

  def hadoopConfiguration = ScriptSQLExec.context().execListener.sparkSession.sparkContext.hadoopConfiguration

  def readFile(conf:Configuration,path: String): String = {
    val fs = FileSystem.get(conf)
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

  def getFileStatus(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    val file = fs.getFileStatus(new Path(path))
    file
  }


  def readAsInputStream(conf:Configuration,fileName: String): InputStream = {
    val fs = FileSystem.get(conf)
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


  def readBytes(conf:Configuration,fileName: String): Array[Byte] = {
    val fs = FileSystem.get(conf)
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

  def listModelDirectory(conf:Configuration,path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(conf)
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def listFiles(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path))
  }

  def saveBytesFile(conf:Configuration,path: String, fileName: String, bytes: Array[Byte]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(conf:Configuration)
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

  def saveStream(conf:Configuration,path: String, fileName: String, inputStream: InputStream) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(conf)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      IOUtils.copyBytes(inputStream, dos, 4 * 1024 * 1024)
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

  def ceateEmptyFile(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    val dos = fs.create(new Path(path))
    dos.close()
  }

  def saveFile(conf:Configuration,path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(conf)
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

  def copyToHDFS(conf:Configuration,tempLocalPath: String, path: String, cleanTarget: Boolean, cleanSource: Boolean) = {
    val fs = FileSystem.get(conf)
    if (cleanTarget) {
      fs.delete(new Path(path), true)
    }
    fs.copyFromLocalFile(new Path(tempLocalPath),
      new Path(path))
    if (cleanSource) {
      FileUtils.forceDelete(new File(tempLocalPath))
    }

  }

  def copyToLocalFile(conf:Configuration,tempLocalPath: String, path: String, clean: Boolean) = {
    val fs = FileSystem.get(conf)
    val tmpFile = new File(tempLocalPath)
    if (tmpFile.exists()) {
      FileUtils.forceDelete(tmpFile)
    }
    fs.copyToLocalFile(new Path(path), new Path(tempLocalPath))
  }

  def deleteDir(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    fs.delete(new Path(path), true)
  }

  def isDir(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    fs.isDirectory(new Path(path))
  }

  def isFile(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    fs.isFile(new Path(path))
  }

  def fileExists(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def createDir(conf:Configuration,path: String) = {
    val fs = FileSystem.get(conf)
    fs.mkdirs(new Path(path))
  }

  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    val dir = "/tmp/train/" + Md5.md5Hash(path)
    if (autoCreateParentDir) {
      FileUtils.forceMkdir(new File(dir))
    }
    dir
  }

  def iteratorFiles(conf:Configuration,path: String, recursive: Boolean) = {
    val fs = FileSystem.get(conf)
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
