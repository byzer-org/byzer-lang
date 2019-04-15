/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.common

import java.io.{BufferedReader, ByteArrayOutputStream, InputStream, InputStreamReader}
import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FSDataInputStream,FileStatus,FSDataOutputStream}
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable.ArrayBuffer

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

  def getFileStatus(path: String) = {
    val fs = FileSystem.get(new Configuration())
    val file = fs.getFileStatus(new Path(path))
    file
  }


  def readAsInputStream(fileName: String): InputStream = {
    val fs = FileSystem.get(new Configuration())
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
    val fs = FileSystem.get(new Configuration())
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
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def listFiles(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path))
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

  def saveStream(path: String, fileName: String, inputStream: InputStream) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
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

  def ceateEmptyFile(path: String) = {
    val fs = FileSystem.get(new Configuration())
    val dos = fs.create(new Path(path))
    dos.close()
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

  def getFilePath(path: String) = {
    new Path(path).toString
  }

  def copyToHDFS(tempLocalPath: String, path: String, cleanTarget: Boolean, cleanSource: Boolean) = {
    val fs = FileSystem.get(new Configuration())
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
    val fs = FileSystem.get(new Configuration())
    val tmpFile = new File(tempLocalPath)
    if (tmpFile.exists()) {
      FileUtils.forceDelete(tmpFile)
    }
    fs.copyToLocalFile(new Path(path), new Path(tempLocalPath))
  }

  def deleteDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }

  def isDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.isDirectory(new Path(path))
  }

  def isFile(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.isFile(new Path(path))
  }

  def fileExists(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.exists(new Path(path))
  }

  def createDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
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
    val fs = FileSystem.get(new Configuration())
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


  def main(args: Array[String]): Unit = {
    println(readFile("file:///Users/allwefantasy/streamingpro/flink.json"))
  }
}
