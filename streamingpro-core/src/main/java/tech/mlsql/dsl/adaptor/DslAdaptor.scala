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

package tech.mlsql.dsl.adaptor

import org.antlr.v4.runtime.misc.Interval
import org.apache.commons.lang3.StringUtils
import streaming.dsl._
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.{ExpressionContext, SqlContext}

import java.net.{URI, URL}

/**
 * Created by allwefantasy on 27/8/2017.
 */
trait DslAdaptor extends DslTool {
  def parse(ctx: SqlContext): Unit
}

trait DslTool {

  def branchContext = {
    ScriptSQLExec.context().execListener.branchContext.contexts
  }

  def currentText(ctx: SqlContext) = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    input.getText(interval)
  }

  def cleanStr(str: String) = {
    if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''")))
      str.substring(1, str.length - 1)
    else str
  }


  def cleanBlockStr(str: String) = {
    if (str.startsWith("'''") && str.endsWith("'''"))
      str.substring(3, str.length - 3)
    else str
  }

  def getStrOrBlockStr(ec: ExpressionContext) = {
    if (ec.STRING() == null || ec.STRING().getText.isEmpty) {
      cleanBlockStr(ec.BLOCK_STRING().getText)
    } else {
      cleanStr(ec.STRING().getText)
    }
  }

  def withPathPrefix(prefix: String, path: String): String = {
    var filePathSchemas = Array[String]()
    if (ScriptSQLExec.context() != null) {
      val session = ScriptSQLExec.context().execListener.sparkSession
      filePathSchemas = session.conf.get("spark.mlsql.path.schemas", "").
        split(",").map(_.trim).filterNot(_.isEmpty)
    }

    val newPath = cleanStr(path)
    if (prefix.isEmpty) return newPath

    if (newPath.contains("..")) {
      throw new RuntimeException("path should not contains ..")
    }

    if (newPath.startsWith("/")) {
      return prefix + newPath.substring(1, newPath.length)
    }

    val currentSchema = extractSchema(path)
    if (currentSchema.isDefined && filePathSchemas.contains(currentSchema.get)) {
      return path
    }
    prefix + newPath
  }

  def withPathPrefix(context: MLSQLExecuteContext, path: String): String = {
    withPathPrefix(context.home, path)
  }

  def parseDBAndTableFromStr(str: String) = {
    val cleanedStr = cleanStr(str)
    val dbAndTable = cleanedStr.split("\\.")
    if (dbAndTable.length > 1) {
      val db = dbAndTable(0)
      val table = dbAndTable.splitAt(1)._2.mkString(".")
      (db, table)
    } else {
      (cleanedStr, cleanedStr)
    }

  }

  def extractSchema(path: String): Option[String] = {

    if (StringUtils.isBlank(path)){
      return None
    }

    if (path.toLowerCase.startsWith("file:/")) {
      return Some("file")
    }

    var c = 0
    val l = path.length
    var index = -1

    while (c < l && index == -1) {
      if (path(c) == ':' && (
        (c + 1 < l
          && path(c + 1) == '/'
          && c + 2 < l && path(c + 2) == '/') ||
          (c + 1 < l
            && path(c + 1) == '\\'
            && c + 2 < l && path(c + 2) == '\\')
        )) {
        index = c
      }
      c += 1
    }
    if (index != -1) Some(path.substring(0, index))
    else None
  }

  /**
   * we need calculate the real absolute path of resource.
   * resource path = owner path prefix + input path
   *
   * @param scriptSQLExecListener script sql execute listener, which contains owner and owner path prefix relationship.
   * @param resourceOwner         resource owner
   * @param path                  resource relative path
   * @return
   */
  def resourceRealPath(scriptSQLExecListener: ScriptSQLExecListener,
                       resourceOwner: Option[String],
                       path: String): String = {
    withPathPrefix(scriptSQLExecListener.pathPrefix(resourceOwner), cleanStr(path))
  }

  def parseRef(format: String, path: String, separator: String, callback: Map[String, String] => Unit) = {
    var finalPath = path
    var dbName = ""

    val firstIndex = finalPath.indexOf(separator)

    if (firstIndex > 0) {

      dbName = finalPath.substring(0, firstIndex)
      finalPath = finalPath.substring(firstIndex + 1)
      ConnectMeta.presentThenCall(DBMappingKey(format, dbName), options => callback(options))
    }

    Array(dbName, finalPath)
  }

  /**
   * The `runtime.hadoop` prefix is used to set spark multiple options, and the prefix is removed globally and set to options.
   * @param runtimeOptions the runtime hadoop options
   * @return
   */
  def cleanRuntimeOptionsPrefix(runtimeOptions: Map[String, String]): Map[String, String] = {
    runtimeOptions.map { item =>
      (item._1.replace("runtime.hadoop.", ""), item._2)
    }
  }
}
