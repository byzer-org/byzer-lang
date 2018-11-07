package streaming.dsl

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import org.antlr.v4.runtime.tree.{ParseTreeWalker}
import org.antlr.v4.runtime._
import org.apache.spark.sql.SparkSession
import streaming.dsl.auth._
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLListener, DSLSQLParser}
import streaming.dsl.parser.DSLSQLParser._
import streaming.log.{Logging, WowLog}
import streaming.parser.lisener.BaseParseListenerextends

import scala.collection.mutable.ArrayBuffer


/**
  * Created by allwefantasy on 25/8/2017.
  */
object ScriptSQLExec extends Logging with WowLog {

  //dbName -> (format->jdbc,url->....)
  val dbMapping = new ConcurrentHashMap[String, Map[String, String]]()

  def options(name: String, _options: Map[String, String]) = {
    dbMapping.put(name, _options)
  }

  private[this] val mlsqlExecuteContext: ThreadLocal[MLSQLExecuteContext] = new ThreadLocal[MLSQLExecuteContext]

  def context(): MLSQLExecuteContext = mlsqlExecuteContext.get

  def contextGetOrForTest(): MLSQLExecuteContext = {
    if (context() == null) {
      val exec = new ScriptSQLExecListener(null, "/tmp/william", Map())
      setContext(new MLSQLExecuteContext("testUser", exec.pathPrefix(None), Map()))
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


  def parse(input: String, listener: DSLSQLListener, skipInclude: Boolean = true, skipAuth: Boolean = true, skipPhysicalJob: Boolean = false) = {
    //preprocess some statements e.g. include
    var wow = input

    var max_preprocess = 10
    var stop = false

    if (!skipInclude) {
      val sqel = listener.asInstanceOf[ScriptSQLExecListener]
      while (!stop && max_preprocess > 0) {
        val preProcessListener = new PreProcessIncludeListener(sqel._sparkSession, sqel._defaultPathPrefix, sqel._allPathPrefix)
        sqel.includeProcessListner = Some(preProcessListener)
        _parse(wow, preProcessListener)
        val includes = preProcessListener.includes()
        if (includes.size == 0) {
          stop = true
        }
        includes.foreach { f =>
          wow = wow.replace(f._1, f._2.substring(0, f._2.lastIndexOf(";")))
        }
        max_preprocess -= 1
      }
    }
    if (!skipAuth) {
      val sqel = listener.asInstanceOf[ScriptSQLExecListener]
      val setListener = new PreProcessSetListener(sqel._sparkSession, sqel._defaultPathPrefix, sqel._allPathPrefix)
      sqel.setProcessListner = Some(setListener)
      _parse(input, setListener)
      // setListener.env()
      val authListener = new AuthProcessListener(setListener)
      sqel.authProcessListner = Some(authListener)
      _parse(input, authListener)
      val tableAuth = Class.forName(authListener.listener.env().getOrElse("__auth_client__", "streaming.dsl.auth.meta.client.DefaultConsoleClient")).newInstance().asInstanceOf[TableAuth]
      tableAuth.auth(authListener.tables().tables.toList)
    }

    if (!skipPhysicalJob) {
      _parse(wow, listener)
    }
  }

  def _parse(input: String, listener: DSLSQLListener) = {
    val loadLexer = new DSLSQLLexer(new CaseChangingCharStream(input))
    val tokens = new CommonTokenStream(loadLexer)
    val parser = new DSLSQLParser(tokens)
    parser.addErrorListener(new BaseErrorListener {
      override def syntaxError(recognizer: Recognizer[_, _],
                               offendingSymbol:
                               scala.Any,
                               line: Int,
                               charPositionInLine: Int,
                               msg: String,
                               e: RecognitionException): Unit = {
        logInfo(format(s"MLSQL Parser error ${msg}"), e)
        throw new RuntimeException(s"MLSQL Parser error : $msg")
      }
    })
    val stat = parser.statement()
    ParseTreeWalker.DEFAULT.walk(listener, stat)
  }
}


class ScriptSQLExecListener(val _sparkSession: SparkSession, val _defaultPathPrefix: String, val _allPathPrefix: Map[String, String]) extends BaseParseListenerextends {

  private val _env = new scala.collection.mutable.HashMap[String, String]

  private val lastSelectTable = new AtomicReference[String]()

  var grammarProcessListener: Option[GrammarProcessListener] = Some(new GrammarProcessListener(this))
  var includeProcessListner: Option[PreProcessIncludeListener] = None
  var setProcessListner: Option[PreProcessSetListener] = None
  var authProcessListner: Option[AuthProcessListener] = None


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
      } else {
        return pathPrefix.get + "/"
      }
    } else if (_defaultPathPrefix != null && _defaultPathPrefix.nonEmpty) {
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

    ctx.getChild(0).getText.toLowerCase() match {
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
        new SetAdaptor(this).parse(ctx)
      case "train" | "run" | "predict" =>
        new TrainAdaptor(this).parse(ctx)
      case "register" =>
        new RegisterAdaptor(this).parse(ctx)
    }

  }

}

class GrammarProcessListener(scriptSQLExecListener: ScriptSQLExecListener) extends BaseParseListenerextends {
  override def exitSql(ctx: SqlContext): Unit = {
  }
}

class AuthProcessListener(val listener: ScriptSQLExecListener) extends BaseParseListenerextends {
  private val _tables = MLSQLTableSet(ArrayBuffer[MLSQLTable]())

  def addTable(table: MLSQLTable) = {
    _tables.tables.asInstanceOf[ArrayBuffer[MLSQLTable]] += table
  }

  def withDBs = {
    _tables.tables.filter(f => f.db.isDefined)
  }

  def withoutDBs = {
    _tables.tables.filterNot(f => f.db.isDefined)
  }

  def tables() = _tables

  override def exitSql(ctx: SqlContext): Unit = {
    ctx.getChild(0).getText.toLowerCase() match {
      case "load" =>
        new LoadAuth(this).auth(ctx)

      case "select" =>
        new SelectAuth(this).auth(ctx)

      case "save" =>
        new SaveAuth(this).auth(ctx)

      case "connect" =>

      case "create" =>
        new CreateAuth(this).auth(ctx)
      case "insert" =>
        new InsertAuth(this).auth(ctx)
      case "drop" =>
        new DropAuth(this).auth(ctx)
      case "refresh" =>

      case "set" =>

      case "train" | "run" =>

      case "register" =>

    }
  }
}


class PreProcessSetListener(_sparkSession: SparkSession,
                            _defaultPathPrefix: String,
                            _allPathPrefix: Map[String, String])
  extends ScriptSQLExecListener(_sparkSession, _defaultPathPrefix, _allPathPrefix) {

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case "set" =>
        new SetAdaptor(this).parse(ctx)
      case _ =>
    }

  }
}


class PreProcessIncludeListener(_sparkSession: SparkSession,
                                _defaultPathPrefix: String,
                                _allPathPrefix: Map[String, String]) extends
  ScriptSQLExecListener(_sparkSession, _defaultPathPrefix, _allPathPrefix) {
  private val _includes = new scala.collection.mutable.HashMap[String, String]

  def addInclude(k: String, v: String) = {
    _includes(k) = v
    this
  }

  def includes() = _includes

  override def exitSql(ctx: SqlContext): Unit = {

    ctx.getChild(0).getText.toLowerCase() match {
      case "include" =>
        new IncludeAdaptor(this).parse(ctx)
      case _ =>
    }

  }
}

case class MLSQLExecuteContext(owner: String, home: String, userDefinedParam: Map[String, String] = Map())


