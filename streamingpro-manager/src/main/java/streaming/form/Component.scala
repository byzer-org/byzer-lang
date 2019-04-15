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

package streaming.form

import streaming.db.TSparkJobParameter

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 12/7/2017.
  */


object AppInstallParameterActionType extends Enumeration {
  type AppInstallParameterActionType = Value
  val NODES = Value("nodes")
  val NORMAL = Value("normal")
  val DEPENDENCY = Value("dependency")
  val PASSWORD = Value("password")
  val MasterHost = Value("masterHost")
  val SELECT = Value("select")
}

object FormType extends Enumeration {
  type FormType = Value
  val SELECT = Value("select")
  val NORMAL = Value("normal")
  val CHECKBOX = Value("checkbox")
}

object HtmlHelper {
  def link(url: String, name: String, style: String = "", target: String = "_blank") = {
    s""" <a href="${url}" class="${style}">${name}</a>  """
  }

  def button(name: String, style: String) = {
    s"""<button class="btn ${style} btn-lg">${name}</button>"""
  }
}








