package tech.mlsql.lang.cmd.compile.internal.gc.test

import org.scalatest.FunSuite
import tech.mlsql.lang.cmd.compile.internal.gc._

/**
 * 2/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ScannerTest extends FunSuite {

  def want(items: List[Token], index: Int, t: Scanner.TokenType, str: String) = {
    assert(items(index).t == t && items(index).text == str)
  }

  test("tokenizer") {
    val items = Tokenizer.tokenize(""" :jack=="jack" and :bj>=24 """)
    want(items, 0, Scanner.Variable, ":jack")
    want(items, 1, Scanner.Eql, "==")
    want(items, 2, Scanner.String, "\"jack\"")
    want(items, 3, Scanner._And, "and")
    want(items, 4, Scanner.Variable, ":bj")
    want(items, 5, Scanner.Geq, ">=")
    want(items, 6, Scanner.Int, "24")

  }

  test("tokenizer2") {
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

  test("ast") {
    val scanner = new Scanner(""" :jack=="jack" and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int))).toString)

  }

  test("ast2") {
    val scanner = new Scanner(""" 1 + 2 * 3""")
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == Add(Literal(1, Types.Int), Mul(Literal(2, Types.Int), Literal(3, Types.Int))).toString)

  }

  test("ast4") {
    val scanner = new Scanner(""" (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parseStatement().asInstanceOf[Expression]
    assert(ast.toString == AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int))).toString)

  }

  test("ast5") {
    val scanner = new Scanner(""":dj == "" ; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val ast = parser.parse().asInstanceOf[List[Expression]]
    assert(ast.toString == List(Eql(Variable(":dj",Types.Any),Literal("\"\"",Types.String)),AndAnd(
      Eql(Variable(":jack", Types.Any), Literal("\"jack\"", Types.String)),
      Geq(Variable(":bj", Types.Any), Literal(24, Types.Int)))).toString)

  }

  test("ast6") {
    val scanner = new Scanner("""select split(:a,",") as :jack; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }
  test("ast7") {
    val scanner = new Scanner("""select split(:a,",") as :jack,"" as :jack1; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

  test("ast8") {
    val scanner = new Scanner("""select split(:a,",")[0] as :jack """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

  test("ast9") {
    val scanner = new Scanner("""select :table[0] as :jack """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

  test("ast10") {
    val scanner = new Scanner(
      """
        |select split(:a,",")[0] as :jack,"" as :jack1;
        |(:jack=="jack" and 1==1) and :bj>=24
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    //    val items = Tokenizer.tokenize("""
    //                         |select split(:a,",")[0] as :jack,"" as :jack1;
    //                         |(:jack=="jack" and 1==1) and :bj>=24
    //                         |""".stripMargin)
    //    items.foreach(item=>println(item.text))
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

  test("ast11") {
    val scanner = new Scanner(
      """
        |select cast(:a as int) as :jack;
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

  test("ast12") {
    val scanner = new Scanner(
      """
        |split(:a,",")[0] == "jack"
        |""".stripMargin)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

  }

}
