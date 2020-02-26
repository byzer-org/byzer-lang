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

package org.apache.spark.util

/**
  * Created by allwefantasy on 23/3/2017.
  */
import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.util.BatchSourceFile
import scala.tools.nsc.io.{VirtualDirectory, AbstractFile}
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.collection.mutable
import java.io.File

object Compiler {
  val compiler = new Compiler(None)

  def eval[T](script:String,className:String) = {
    compiler.eval[T](script,className:String)
  }
}

class Compiler(targetDir: Option[File]) {

  val target = targetDir match {
    case Some(dir) => AbstractFile.getDirectory(dir)
    case None => new VirtualDirectory("(memory)", None)
  }

  val classCache = mutable.Map[String, Class[_]]()

  private val settings = new Settings()
  settings.deprecation.value = true // enable detailed deprecation warnings
  settings.unchecked.value = true // enable detailed unchecked warnings
  settings.outputDirs.setSingleOutput(target)
  settings.usejavacp.value = true

  private val global = new Global(settings)
  private lazy val run = new global.Run

  val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)

  /**Compiles the code as a class into the class loader of this compiler.
    *
    * @param code
    * @return
    */
  def compile(code: String,className:String) = {
    val sourceFiles = List(new BatchSourceFile("(inline)", code))
    run.compileSources(sourceFiles)
    findClass(className).get
  }

  /** Compiles the source string into the class loader and
    * evaluates it.
    *
    * @param code
    * @tparam T
    * @return
    */
  def eval[T](code: String,className:String): T = {
    val cls = compile(code,className)
    cls.getConstructor().newInstance().asInstanceOf[T]
  }

  def findClass(className: String): Option[Class[_]] = {
    synchronized {
      classCache.get(className).orElse {
        try {
          val cls = classLoader.loadClass(className)
          classCache(className) = cls
          Some(cls)
        } catch {
          case e: ClassNotFoundException => None
        }
      }
    }
  }
}