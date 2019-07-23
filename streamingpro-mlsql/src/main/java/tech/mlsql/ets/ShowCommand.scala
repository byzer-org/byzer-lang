package tech.mlsql.ets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.dsl.CommandCollection

/**
  * 2019-04-12 WilliamZhu(allwefantasy@gmail.com)
  */
class ShowCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    def cleanPath = {
      path.split("/").filterNot(f => f.isEmpty)
    }

    def help = {
      val context = ScriptSQLExec.contextGetOrForTest()
      context.execListener.addEnv("__content__",
        """
          |command
          |!show jobs;
          |!show "jobs/[groupId]"
          |!show datasources;
          |!show "datasources/params/[datasource name]";
          |!show resource;
          |!show "resource/[groupId]";
          |!show "progress/[groupId]";
          |!show et;
          |!show et [ETName];
          |!show functions;
          |!show function [functionName];
          |!show tables;
          |!show tables from [database];
        """.stripMargin)

      s"""
         |set __content__='''
         |${context.execListener.env()("__content__")}
         |''';
         |load csvStr.`__content__` where header="true" as __output__;
         """.stripMargin
    }

    val newPath = cleanPath
    val sql = newPath match {
      case Array("et", name) => s"load modelExample.`${name}` as __output__;"
      case Array("et") => s"load modelList.`` as __output__;"
      case Array("functions") => s"run command as ShowFunctionsExt.``;"
      case Array("function", name) => s"run command as ShowFunctionsExt.`${name}`;"
      case Array("tables") => s"run command as ShowTablesExt.``;"
      case Array("tables" ,"named" ,aliasName) => s"run command as ShowTablesExt.`` as ${aliasName};"
      case Array("tables" ,"from" ,name) => s"run command as ShowTablesExt.`${name}`;"
      case Array("tables" ,"from" ,name ,"named" ,aliasName) => s"run command as ShowTablesExt.`` as ${aliasName};"
      case Array("commands") | Array() | Array("help") | Array("-help") =>
        help
      case _ => s"load _mlsql_.`${newPath.mkString("/")}` as output;"
    }
    CommandCollection.evaluateMLSQL(df.sparkSession, sql)
  }


  override def skipPathPrefix: Boolean = true

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }
}
