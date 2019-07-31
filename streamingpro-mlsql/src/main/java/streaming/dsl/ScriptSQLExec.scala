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

package streaming.dsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.apache.spark.MLSQLSyntaxErrorListener
import org.apache.spark.sql.SparkSession
import streaming.core.Dispatcher
import streaming.dsl.auth._
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLListener, DSLSQLParser}
import streaming.log.{Logging, WowLog}
import streaming.parser.lisener.BaseParseListenerextends
import tech.mlsql.Stage
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.dsl.adaptor._
import tech.mlsql.dsl.parser.MLSQLErrorStrategy
import tech.mlsql.dsl.processor.{AuthProcessListener, GrammarProcessListener, PreProcessListener}
import tech.mlsql.job.MLSQLJobProgressListener

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 25/8/2017.
  */
object ScriptSQLExec extends Logging with WowLog {

  private[this] val mlsqlExecuteContext: ThreadLocal[MLSQLExecuteContext] = new ThreadLocal[MLSQLExecuteContext]

  def context(): MLSQLExecuteContext = mlsqlExecuteContext.get

  def contextGetOrForTest(): MLSQLExecuteContext = {
    if (context() == null) {
      logError("If this is not unit test, then it may be  something wrong. context should not be null")
      val exec = new ScriptSQLExecListener(null, "/tmp/william", Map())
      setContext(new MLSQLExecuteContext(exec, "testUser", exec.pathPrefix(None), "", Map()))
    }
    context()
  }

  def setContext(ec: MLSQLExecuteContext): Unit = mlsqlExecuteContext.set(ec)

  def setContextIfNotPresent(ec: MLSQLExecuteContext): Unit = {
    if (ScriptSQLExec.context() == null) {
      mlsqlExecuteContext.set(ec)
    }
  }

  def unset = mlsqlExecuteContext.remove()


  def parse(input: String, listener: DSLSQLListener,
            skipInclude: Boolean = true,
            skipAuth: Boolean = true,
            skipPhysicalJob: Boolean = false,
            skipGrammarValidate: Boolean = true
           ) = {
    //preprocess some statements e.g. include

    var wow = input

    var max_preprocess = 10
    var stop = false

    val sqel = listener.asInstanceOf[ScriptSQLExecListener]
    CommandCollection.fill(sqel)
    if (!skipInclude) {
      sqel.setStage(Stage.include)
      while (!stop && max_preprocess > 0) {
        val preProcessListener = new PreProcessIncludeListener(sqel)
        sqel.includeProcessListner = Some(preProcessListener)
        _parse(wow, preProcessListener)

        val newScript = preProcessListener.toScript
        if (newScript == wow) {
          stop = true
        }
        wow = newScript
        max_preprocess -= 1
      }
    }

    val preProcessListener = new PreProcessListener(sqel)
    sqel.preProcessListener = Some(preProcessListener)
    sqel.setStage(Stage.preProcess)
    _parse(wow, preProcessListener)
    wow = preProcessListener.toScript

    if (!skipGrammarValidate) {
      sqel.setStage(Stage.grammar)
      _parse(wow, new GrammarProcessListener(sqel))
    }

    if (!skipAuth) {
      val staticAuthImpl = sqel.sparkSession
        .sparkContext
        .getConf
        .getOption("spark.mlsql.auth.implClass")

      val authListener = new AuthProcessListener(sqel)
      sqel.authProcessListner = Some(authListener)
      sqel.setStage(Stage.auth)
      _parse(wow, authListener)

      val authImpl = staticAuthImpl match {
        case Some(temp) => temp
        case None => context.userDefinedParam.getOrElse("__auth_client__",
          Dispatcher.contextParams("").getOrDefault("context.__auth_client__", "streaming.dsl.auth.meta.client.DefaultConsoleClient").toString)
      }
      val tableAuth = Class.forName(authImpl)
        .newInstance().asInstanceOf[TableAuth]
      sqel.setTableAuth(tableAuth)
      tableAuth.auth(authListener.tables().tables.toList)
    }

    if (!skipPhysicalJob) {
      sqel.setStage(Stage.physical)
      _parse(wow, listener)
    }
  }

  def _parse(input: String, listener: DSLSQLListener) = {
    val loadLexer = new DSLSQLLexer(new CaseChangingCharStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)

    parser.setErrorHandler(new MLSQLErrorStrategy)
    parser.addErrorListener(new MLSQLSyntaxErrorListener())

    val stat = parser.statement()
    ParseTreeWalker.DEFAULT.walk(listener, stat)
  }
}


class ScriptSQLExecListener(val _sparkSession: SparkSession, val _defaultPathPrefix: String, val _allPathPrefix: Map[String, String]) extends BaseParseListenerextends {

  private val _env = new scala.collection.mutable.HashMap[String, String]

  private[this] val _jobListeners = ArrayBuffer[MLSQLJobProgressListener]()

  private val lastSelectTable = new AtomicReference[String]()
  private[this] var _tableAuth: AtomicReference[TableAuth] = new AtomicReference[TableAuth]()

  def addJobProgressListener(l: MLSQLJobProgressListener) = {
    _jobListeners += l
    this
  }

  var includeProcessListner: Option[PreProcessIncludeListener] = None
  var preProcessListener: Option[PreProcessListener] = None
  var authProcessListner: Option[AuthProcessListener] = None

  private var stage: Option[Stage.stage] = None


  def clone(sparkSession: SparkSession): ScriptSQLExecListener = {
    val ssel = new ScriptSQLExecListener(sparkSession, _defaultPathPrefix, _allPathPrefix)
    _env.foreach { case (a, b) => ssel.addEnv(a, b) }
    if (getStage.isDefined) {
      ssel.setStage(getStage.get)
    }
    if (getTableAuth.isDefined) {
      setTableAuth(getTableAuth.get)
    }

    ssel.includeProcessListner = includeProcessListner
    ssel.preProcessListener = preProcessListener
    ssel.authProcessListner = authProcessListner

    ssel
  }

  def setStage(_stage: Stage.stage) = {
    stage = Option(_stage)
  }

  def getStage = {
    stage
  }

  def setTableAuth(tableAuth: TableAuth): Unit = {
    _tableAuth.set(tableAuth)
  }

  def getTableAuth: Option[TableAuth] = {
    Option(_tableAuth.get())
  }

  def setLastSelectTable(table: String) = {
    lastSelectTable.set(table)
  }

  def getLastSelectTable() = {
    if (lastSelectTable.get() == null) None else Some(lastSelectTable.get())
  }


  def addEnv(k: String, v: String) = {
    _env(k) = v
    this
  }

  def env() = _env

  def sparkSession = _sparkSession

  def pathPrefix(owner: Option[String]): String = {

    if (_allPathPrefix != null && _allPathPrefix.nonEmpty && owner.isDefined) {
      val pathPrefix = _allPathPrefix.get(owner.get)
      if (pathPrefix.isDefined && pathPrefix.get.endsWith("/")) {
        return pathPrefix.get
      } else if (pathPrefix.isDefined && !pathPrefix.get.endsWith("/")) {
        return pathPrefix.get + "/"
      }
    }

    if (_defaultPathPrefix != null && _defaultPathPrefix.nonEmpty) {
      if (_defaultPathPrefix.endsWith("/")) {
        return _defaultPathPrefix
      } else {
        return _defaultPathPrefix + "/"
      }
    } else {
      return ""
    }
  }

  override def exitSql(ctx: SqlContext): Unit = {

    def getText = {
      val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

      val start = ctx.start.getStartIndex()
      val stop = ctx.stop.getStopIndex()
      val interval = new Interval(start, stop)
      input.getText(interval)
    }

    def before(clzz: String) = {
      _jobListeners.foreach(_.before(clzz, getText))
    }

    def after(clzz: String) = {
      _jobListeners.foreach(_.after(clzz, getText))
    }


    val PREFIX = ctx.getChild(0).getText.toLowerCase()

    before(PREFIX)
    PREFIX match {
      case "load" =>
        new LoadAdaptor(this).parse(ctx)

      case "select" =>
        new SelectAdaptor(this).parse(ctx)

      case "save" =>
        new SaveAdaptor(this).parse(ctx)

      case "connect" =>
        new ConnectAdaptor(this).parse(ctx)
      case "create" =>
        new CreateAdaptor(this).parse(ctx)
      case "insert" =>
        new InsertAdaptor(this).parse(ctx)
      case "drop" =>
        new DropAdaptor(this).parse(ctx)
      case "refresh" =>
        new RefreshAdaptor(this).parse(ctx)
      case "set" =>
        new SetAdaptor(this, Stage.physical).parse(ctx)
      case "train" | "run" | "predict" =>
        new TrainAdaptor(this).parse(ctx)
      case "register" =>
        new RegisterAdaptor(this).parse(ctx)
      case _ => throw new RuntimeException(s"Unknow statement:${ctx.getText}")
    }
    after(PREFIX)

  }

}


object ConnectMeta {
  //dbName -> (format->jdbc,url->....)
  private val dbMapping = new ConcurrentHashMap[DBMappingKey, Map[String, String]]()

  def options(key: DBMappingKey, _options: Map[String, String]) = {
    dbMapping.put(key, _options)
  }

  def options(key: DBMappingKey) = {
    if (dbMapping.containsKey(key)) {
      Option(dbMapping.get(key))
    } else None
  }

  def presentThenCall(key: DBMappingKey, f: Map[String, String] => Unit) = {
    if (dbMapping.containsKey(key)) {
      val item = dbMapping.get(key)
      f(item)
      Option(item)
    } else None
  }
}

case class MLSQLExecuteContext(@transient execListener: ScriptSQLExecListener,
                               owner: String,
                               home: String,
                               groupId: String,
                               userDefinedParam: Map[String, String] = Map())

case class DBMappingKey(format: String, db: String)





