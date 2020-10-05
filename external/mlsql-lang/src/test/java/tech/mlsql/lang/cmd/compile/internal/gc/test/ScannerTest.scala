package tech.mlsql.lang.cmd.compile.internal.gc.test

import org.scalatest.FunSuite
import tech.mlsql.lang.cmd.compile.internal.gc.{Expression, Scanner, Select, StatementParser, Tokenizer, Variable}

/**
 * 2/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ScannerTest extends FunSuite {
  test("tokenizer") {
    Tokenizer.tokenize(""" :jack=="jack" and :bj>=24 """).foreach { token =>
      println(s"${token.text} ${token.t}")
    }
  }

  test("tokenizer2") {
    Tokenizer.tokenize("""select split(:a,",") as :jack;""").foreach { token =>
      println(s"${token.text} ${token.t}")
    }
  }

  test("ast") {
    val scanner = new Scanner(""" :jack=="jack" and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parseStatement())

  }

  test("ast2") {
    val scanner = new Scanner(""" 1 + 2 * 3""")
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parseStatement())

  }

  test("ast3") {
    val scanner = new Scanner(""" :jack=="jack" and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parseStatement())

  }
  test("ast4") {
    val scanner = new Scanner(""" (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parseStatement())

  }

  test("ast5") {
    val scanner = new Scanner(""":dj == "" ; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    println(parser.parse())

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

  test("transformDown") {
    val scanner = new Scanner("""select split(:a,",") as :jack,"" as :jack1; (:jack=="jack") and :bj>=24 """)
    val tokenizer = new Tokenizer(scanner)
    val parser = new StatementParser(tokenizer)
    val exprs = parser.parse()
    exprs.foreach { item =>
      item.collect[Expression]{
        case a@Variable(name,_)=>
          println(name)
          a
      }
//      item.transformDown{
//        case Select(items)=>items
//
//      }
    }
  }
}
