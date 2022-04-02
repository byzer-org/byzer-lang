package tech.mlsql.runtime

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import tech.mlsql.lang.cmd.compile.internal.gc._

import scala.collection.mutable.ArrayBuffer

/**
 * 2/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ScannerAndParserTest extends AnyFlatSpec with should.Matchers {

  def want(items: List[Token], index: Int, t: Scanner.TokenType, str: String) = {
    assert(items(index).t == t && items(index).text == str)
  }

  "A tokenizer" should "return then mock result" in{
    val items = Tokenizer.tokenize(""" :jack=="jack" and :bj>=24 """)
    want(items, 0, Scanner.Variable, ":jack")
    want(items, 1, Scanner.Eql, "==")
    want(items, 2, Scanner.String, "\"jack\"")
    want(items, 3, Scanner._And, "and")
    want(items, 4, Scanner.Variable, ":bj")
    want(items, 5, Scanner.Geq, ">=")
    want(items, 6, Scanner.Int, "24")

  }

  "A tokenizer2" should "return then mock result" in{
    val items = Tokenizer.tokenize("""select split(:a,",") as :jack;""")
    want(items, 0, Scanner._SELECT, "select")
    want(items, 1, Scanner.Ident, "split")
    want(items, 2, Scanner.Lparen, "(")
    want(items, 3, Scanner.Variable, ":a")
    want(items, 4, Scanner.Comma, ",")
    want(items, 5, Scanner.String, "\",\"")
    want(items, 6, Scanner.Rparen, ")")
    want(items, 7, Scanner._As, "as")
    want(items, 8, Scanner.Variable, ":jack")
    //want(items,9,Scanner.Semi,";")
  }

  "An ast" should "return then mock result" in{
    val scanner = new Scanner(""" :jack=="jack" and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int))).toString)

  }

  "An ast2" should "return then mock result" in{
    val scanner = new Scanner(""" 1 + 2 * 3""")
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == Add(Literal(1, Types.Int), Mul(Literal(2, Types.Int), Literal(3, Types.Int))).toString)

  }

  "An ast4" should "return then mock result" in{
    val scanner = new Scanner(""" (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int))).toString)

  }

  "An ast5" should "return then mock result" in{
    val scanner = new Scanner(""":dj == "" ; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse().asInstanceOf[List[Expression]]
    assert(ast.toString == List(Eql(Variable(":dj", Types.Any), Literal("\"\"", Types.String)), AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int)))).toString)

  }

  "An ast6" should "return then mock result" in{
    val scanner = new Scanner("""select split(:a,",") as :jack; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(ast.toString() == List(
      Select(
        List(As(
          Variable(":jack", Types.Any),
          FuncCall(Literal("split", Types.String), ArrayBuffer(Variable(":a", Types.Any), Literal("\",\"", Types.String)))
        ))),
      AndAnd(Eql(Variable(":jack", Types.Any),
        Literal("\"jack\"", Types.String)),
        Geq(Variable(":bj", Types.Any), Literal(24, Types.Int)))).toString())

  }


  "An ast8" should "return then mock result" in{
    val scanner = new Scanner("""select split(:a,",")[0] as :jack """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(ast.toString() == List(
      Select(
        List(As(
          Variable(":jack", Types.Any),
          ArrayIndexer(FuncCall(Literal("split", Types.String), ArrayBuffer(Variable(":a", Types.Any), Literal("\",\"", Types.String))), Literal(0, Types.Int))
        ))
      )
    ).toString())

  }

  "An ast9" should "return then mock result" in{
    val scanner = new Scanner("""select :table[0] as :jack """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(ast.toString() == List(
      Select(List(As(Variable(":jack", Types.Any), ArrayIndexer(Variable(":table", Types.Any), Literal(0, Types.Int)))))
    ).toString()
    )

  }

  "An ast10" should "return then mock result" in{
    val scanner = new Scanner(
      """
        |select split(:a,",")[0] as :jack,"" as :jack1;
        |(:jack=="jack" and 1==1) and :bj>=24
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(
      ast.toString() == List(
        Select(List(
          As(Variable(":jack", Types.Any),
            ArrayIndexer(FuncCall(Literal("split", Types.String), ArrayBuffer(Variable(":a", Types.Any),
              Literal("\",\"", Types.String))), Literal(0, Types.Int))),
          As(Variable(":jack1", Types.Any), Literal("\"\"", Types.String)))),
        AndAnd(
          AndAnd(
            Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
            Eql(Literal(1, Types.Int), Literal(1, Types.Int))),
          Geq(Variable(":bj", Types.Any), Literal(24, Types.Int)))).toString()
    )

  }

  "An ast11" should "return then mock result" in{
    val scanner = new Scanner(
      """
        |select cast(:a as int) as :jack;
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(ast.toString() ==
      List(
        Select(List(
          As(
            Variable(":jack", Types.Any),
            Cast(Variable(":a", Types.Any), Literal("int", Types.String)))
        ))).toString()
    )

  }

  "An ast12" should "return then mock result" in{
    val scanner = new Scanner(
      """
        |split(:a,",")[0] == "jack"
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse()
    assert(ast.toString() ==
      List(Eql(
        ArrayIndexer(
          FuncCall(Literal("split", Types.String), ArrayBuffer(Variable(":a", Types.Any), Literal("\",\"", Types.String))), Literal(0, Types.Int)),
        Literal("\"jack\"", Types.String))
      ).toString()
    )

  }

  def buildParser(str: String) = {
    val scanner = new Scanner(str)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    parser
  }

  "An ast13" should "return then mock result" in{
    val parser = buildParser(
      """
        |split(:a,",")[0] = "jack"
        |""".stripMargin)
    val thrown = intercept[ParserException] {
      parser.parse()
    }
    assert(thrown.getMessage == "Error[2:18]: operator is required instead of '=' ")
  }

  "An ast14" should "return then mock result" in{
    val parser = buildParser(
      " split(:a,\",)[0] = \"jack\" ")

    val thrown = intercept[ParserException] {
      parser.parse()
    }
    assert(thrown.getMessage == "Error[1:26]: literal not terminated")
  }

  "An text template" should "return then mock result" in{
    val str = "select :{:jack} as :name as b;"
    val textTemplate = new TextTemplate(Map("jack" -> "wow"), str)
    val tokens = textTemplate.parse
    println(tokens.map(_.chars.mkString("")))

  }

}
