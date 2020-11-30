package tech.mlsql.test

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.ets.SQLGenContext
import tech.mlsql.lang.cmd.compile.internal.gc._
import tech.mlsql.nativelib.runtime.NativeFuncRule

import scala.collection.mutable

/**
 * 6/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ExprTest extends FunSuite with BeforeAndAfterAll {
  var session: SparkSession = null

  override def beforeAll(): Unit = {
    session = SparkSession.builder().withExtensions(extensions => {
      extensions.injectResolutionRule(session => NativeFuncRule)
    }).
      master("local[*]").
      appName("test").
      getOrCreate()
  }

  override def afterAll(): Unit = {
    if (session != null) {
      session.close()
    }
  }

  test("spark codegen") {
    //    session.experimental.extraOptimizations = Seq(NativeFuncRule)
    //
    // TODO(qwang): We now didn't handle the non-ascii chars.
    val rdd = session.sparkContext.parallelize(Seq(Row.fromSeq(Seq("中国DD"))))
    session.createDataFrame(rdd,StructType(Seq(StructField("value", StringType)))).createOrReplaceTempView("jack")
//    ssession.createDataset[String](Seq("DD"))(ssession.implicits.newStringEncoder).createOrReplaceTempView("jack")
    import org.apache.spark.sql.execution.debug._
    session.sql(""" select lower(value) from jack""").debugCodegen()
    session.sql(""" select lower(value) from jack""").show(false)
    val result = session.sql(""" select lower(value) from jack""").collect()
    assert("中国dd".equals(result.apply(0).get(0)))
  }

  def evaluate(str: String, input: Map[String, String]): Any = {
    val scanner = new Scanner(str)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = parser.parse()
    val sQLGenContext = new SQLGenContext(session)
    var variables = new mutable.HashMap[String, Any]()
    variables ++= input
    val variableTable = VariableTable("wow", variables, new mutable.HashMap[String, Any]())
    val item = sQLGenContext.execute(exprs.map(_.asInstanceOf[Expression]), variableTable)
    return item
  }

  test("codegen1") {

    val input = Map("a" -> "jack,20")
    val item = evaluate(
      """
        |select split(:a,",")[0] as :jack,"" as :bj;
        |(:jack=="jack" and 1==1) and :bj>=24
        |""".stripMargin, input)

    assert(item == Literal(false, Types.Boolean))
  }

  test("codegen2") {

    val input = Map("a" -> "jack,20")

    var item = evaluate(
      """
        |select split(:a,",")[0] as :jack,cast(split(:a,",")[1] as float) as :bj;
        |(:jack=="jack" and 1==1) and cast(:bj as int)>=7
        |""".stripMargin, input)

    assert(item == Literal(true, Types.Boolean))

    item = evaluate(
      """
        |select split(:a,",")[0] as :jack,cast(split(:a,",")[1] as float) as :bj;
        |(:jack=="jack" and 1==1) and cast(:bj as int)>=33
        |""".stripMargin, input)

    assert(item == Literal(false, Types.Boolean))

    item = evaluate(
      """
        |select split(:a,",")[0] as :jack,cast(split(:a,",")[1] as float) as :bj;
        |(:jack=="jack1" and 1==1) and cast(:bj as int)>=7
        |""".stripMargin, input)

    assert(item == Literal(false, Types.Boolean))
  }
}
