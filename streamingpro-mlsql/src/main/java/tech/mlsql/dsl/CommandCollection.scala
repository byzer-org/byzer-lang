package tech.mlsql.dsl

import org.apache.spark.sql.SparkSession
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
object CommandCollection {
  def fill(context: ScriptSQLExecListener): Unit = {
    context.addEnv("desc", """run command as ShowTableExt.`{}`""")
    context.addEnv("kill", """run command as Kill.`{}`""")
    context.addEnv("jdbc", """ run command as JDBC.`` where `driver-statement-0`='''{}''' """)

    /*
      -- !cache table1 eagerly with script lifetime
      !cache table1 application
      !uncache table1
     */
    context.addEnv("cache", """ run command as CacheExt.`{}` where lifeTime="{}" """)
    context.addEnv("unCache", """ run command as CacheExt.`{}` where execute="uncache" """)
    context.addEnv("uncache", """ run command as CacheExt.`{}` where execute="uncache" """)

    context.addEnv("show",
      """
        |run command as ShowCommand.`{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}/{}`
      """.stripMargin)
  }

  def evaluateMLSQL(spark: SparkSession, mlsql: String) = {
    val context = new ScriptSQLExecListener(spark, null, null)
    ScriptSQLExec.parse(mlsql, context, true, true)
    spark.table(context.getLastSelectTable().get)
  }
}
