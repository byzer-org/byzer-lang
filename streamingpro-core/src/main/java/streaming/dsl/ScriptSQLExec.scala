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

import _root_.streaming.core.Dispatcher
import _root_.streaming.dsl.auth._
import _root_.streaming.dsl.parser.DSLSQLParser.SqlContext
import _root_.streaming.dsl.parser._
import _root_.streaming.log.WowLog
import _root_.streaming.parser.lisener.BaseParseListener
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.apache.spark.MLSQLSyntaxErrorListener
import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.dsl.adaptor._
import tech.mlsql.dsl.parser.MLSQLErrorStrategy
import tech.mlsql.dsl.processor.{AuthProcessListener, GrammarProcessListener, PreProcessListener}
import tech.mlsql.dsl.scope.ParameterVisibility.ParameterVisibility
import tech.mlsql.dsl.scope.{ParameterVisibility, SetVisibilityParameter}
import tech.mlsql.job.MLSQLJobProgressListener
import tech.mlsql.lang.cmd.compile.internal.gc.VariableTable
import tech.mlsql.session.SetSession
import tech.mlsql.{Stage, session}

import scala.collection.JavaConverters._
import scala.collection.mutable
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
          Dispatcher.contextParams("").getOrDefault("context.__auth_client__", "streaming.dsl.auth.client.DefaultConsoleClient").toString)
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

case class BranchContextHolder(contexts: mutable.Stack[BranchContext], traces: ArrayBuffer[String])

trait BranchContext

case class IfContext(sqls: mutable.ArrayBuffer[DslAdaptor],
                     ctxs: mutable.ArrayBuffer[SqlContext],
                     variableTable: VariableTable,
                     shouldExecute: Boolean,
                     haveMatched: Boolean,
                     skipAll: Boolean) extends BranchContext

case class ForContext() extends BranchContext


class ScriptSQLExecListener(val _sparkSession: SparkSession, val _defaultPathPrefix: String, val _allPathPrefix: Map[String, String]) extends BaseParseListener with Logging with WowLog {

  private val _branchContext = BranchContextHolder(new mutable.Stack[BranchContext](), new ArrayBuffer[String]())

  private val _env = new scala.collection.mutable.HashMap[String, String]

  private val _env_visibility = new scala.collection.mutable.HashMap[String, SetVisibilityParameter]

  private[this] val _jobListeners = ArrayBuffer[MLSQLJobProgressListener]()

  private val lastSelectTable = new AtomicReference[String]()
  private val _declaredTables = new ArrayBuffer[String]()
  private[this] var _tableAuth: AtomicReference[TableAuth] = new AtomicReference[TableAuth]()

  def envSession = new SetSession(_sparkSession, ScriptSQLExec.context().owner)

  def addSessionEnv(k: String, v: String, visibility: String) = {
    envSession.set(k, v, Map[String, String](
      SetSession.__MLSQL_CL__ -> session.SetSession.SET_STATEMENT_CL,
      "visibility" -> visibility
    ))
    this
  }

  def initFromSessionEnv = {

    def getVisibility(_visibility: String) = {
      val visibility = _visibility.split(",")
      val visibilityParameter = visibility.foldLeft(mutable.Set[ParameterVisibility]())((x, y) => {
        y match {
          case "un_select" => x.add(ParameterVisibility.UN_SELECT)
          case _ =>
        }
        x
      })

      if (visibilityParameter.size == 0) {
        visibilityParameter.add(ParameterVisibility.ALL)
      }
      visibilityParameter
    }

    envSession.fetchSetStatement match {
      case Some(items) =>
        items.collect().foreach { item =>
          addEnv(item.k, item.v)
          addEnvVisibility(item.k, SetVisibilityParameter(item.v, getVisibility(item.config("visibility"))))
        }
      case None =>
    }
    this
  }

  def branchContext = {
    _branchContext
  }

  def declaredTables = _declaredTables

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
    _env_visibility.foreach { case (a, b) => ssel.addEnvVisibility(a, b) }
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
    if (table != null) {
      _declaredTables += table
    }
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

  def addEnvVisibility(k: String, v: SetVisibilityParameter) = {
    _env_visibility(k) = v
    this
  }

  def envVisibility() = _env_visibility

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

    def traceBC = {
      ScriptSQLExec.context().execListener.env().getOrElse("__debug__", "false").toBoolean
    }

    def str(ctx: SqlContext) = {

      val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

      val start = ctx.start.getStartIndex()
      val stop = ctx.stop.getStopIndex()
      val interval = new Interval(start, stop)
      input.getText(interval)
    }

    def execute(adaptor: DslAdaptor) = {
      val bc = branchContext.contexts
      if (!bc.isEmpty) {
        bc.pop() match {
          case ifC: IfContext =>
            val isBranchCommand = adaptor match {
              case a: TrainAdaptor =>
                val TrainStatement(_, _, format, _, _, _) = a.analyze(ctx)
                val isBranchCommand = (format == "IfCommand"
                  || format == "ElseCommand"
                  || format == "ElifCommand"
                  || format == "FiCommand"
                  || format == "ThenCommand")
                isBranchCommand
              case _ => false
            }

            if (ifC.skipAll) {
              bc.push(ifC)
              if (isBranchCommand) {
                adaptor.parse(ctx)
              }
            } else {
              if (ifC.shouldExecute && !isBranchCommand) {
                ifC.sqls += adaptor
                ifC.ctxs += ctx
                bc.push(ifC)
              } else if (!ifC.shouldExecute && !isBranchCommand) {
                bc.push(ifC)
                // skip
              }
              else {
                bc.push(ifC)
                adaptor.parse(ctx)
              }
            }
          case forC: ForContext =>
        }
      } else {
        if (traceBC) {
          logInfo(format(s"SQL:: ${str(ctx)}"))
        }
        adaptor.parse(ctx)
      }
    }

    val PREFIX = ctx.getChild(0).getText.toLowerCase()

    before(PREFIX)
    PREFIX match {
      case "load" =>
        val adaptor = new LoadAdaptor(this)
        execute(adaptor)

      case "select" =>
        val adaptor = new SelectAdaptor(this)
        execute(adaptor)

      case "save" =>
        val adaptor = new SaveAdaptor(this)
        execute(adaptor)
      case "connect" =>
        val adaptor = new ConnectAdaptor(this)
        execute(adaptor)
      case "create" =>
        val adaptor = new CreateAdaptor(this)
        execute(adaptor)
      case "insert" =>
        val adaptor = new InsertAdaptor(this)
        execute(adaptor)
      case "drop" =>
        val adaptor = new DropAdaptor(this)
        execute(adaptor)
      case "refresh" =>
        val adaptor = new RefreshAdaptor(this)
        execute(adaptor)
      case "set" =>
        val adaptor = new SetAdaptor(this, Stage.physical)
        execute(adaptor)
      case "train" | "run" | "predict" =>
        val adaptor = new TrainAdaptor(this)
        execute(adaptor)
      case "register" =>
        val adaptor = new RegisterAdaptor(this)
        execute(adaptor)
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

  def toMap = {
    dbMapping.asScala.toMap
  }
}

case class MLSQLExecuteContext(@transient execListener: ScriptSQLExecListener,
                               owner: String,
                               home: String,
                               groupId: String,
                               userDefinedParam: Map[String, String] = Map()
                              )

case class DBMappingKey(format: String, db: String)



