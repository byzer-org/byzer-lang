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
    val url = s"http://docs.mlsql.tech/upload_images/${fileName}"
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
