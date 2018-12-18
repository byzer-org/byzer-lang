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

import java.io.File
import java.net.{URI, URL, URLClassLoader}

import net.csdn.common.logging.Loggers
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


/**
  * Created by allwefantasy on 20/9/2017.
  */
object JarUtil {
  def loadJar(path: String, className: String): Class[_] = {
    if (path.startsWith("hdfs://")) {
      val clzz = new HdfsClassLoader(new Configuration(), new Path(path)).findClass(className)
      return clzz
    }
    val uri = new URI(path).toURL
    val classLoader = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(classLoader, uri)
    return Class.forName(className, true, classLoader)
  }
}

class HdfsURLClassLoader(classLoader: ClassLoader) extends URLClassLoader(Array.ofDim[URL](0), classLoader) {
  val logger = Loggers.getLogger(classOf[HdfsClassLoader])

  def addJarToClasspath(jarName: String) {
    synchronized {
      val conf = new Configuration
      val fileSystem = FileSystem.get(conf)
      val path = new Path(jarName);
      if (!fileSystem.exists(path)) {
        logger.warn(s"File does not exists:$path")
      }
      val uriPath = path.toUri()
      val urlPath = uriPath.toURL()
      addURL(urlPath)
    }
  }
}



