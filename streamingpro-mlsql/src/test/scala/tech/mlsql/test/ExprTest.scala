package tech.mlsql.test

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import tech.mlsql.ets.SQLGenContext
import tech.mlsql.lang.cmd.compile.internal.gc.{Expression, Scanner, StatementParser, Tokenizer}

/**
 * 6/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ExprTest extends FunSuite {
  test("codegen1") {
    val spark = SparkSession.builder().
      master("local[*]").
      appName("test").
      getOrCreate()

    val input = Map("a" -> "jack,20")
    val scanner = new Scanner(
      """
        |select split(:a,",")[0] as :jack,"" as :bj;
        |(:jack=="jack" and 1==1) and :bj>=24
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = parser.parse()
    val sQLGenContext = new SQLGenContext(spark)
    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), input)
    println(item)
    spark.close()
  }

  test("codegen2") {
    val spark = SparkSession.builder().
      master("local[*]").
      appName("test").
      getOrCreate()

    val input = Map("a" -> "jack,20")
    val scanner = new Scanner(
      """
        |select split(:a,",")[0] as :jack,cast(split(:a,",")[1] as float) as :bj;
        |(:jack=="jack" and 1==1) and cast(:bj as int)>=7
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = parser.parse()
    val sQLGenContext = new SQLGenContext(spark)
    println(exprs.head.asInstanceOf[Expression].genCode(sQLGenContext).code)
    println(exprs.last.asInstanceOf[Expression].genCode(sQLGenContext).code)
    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), input)
    println(item)
    spark.close()
  }
}
