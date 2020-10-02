package tech.mlsql.lang.cmd.compile.internal.gc.test

import org.scalatest.FunSuite
import tech.mlsql.lang.cmd.compile.internal.gc.Scanner

/**
 * 2/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class ScannerTest extends FunSuite {
  test("wow") {
    val scanner = new Scanner(""" :jack=="jack" and :bj==24 """)
    scanner.scan
    while (scanner.aheadChar != Scanner.EOF_INT) {
         println(s"${scanner.tokenString()} ${scanner.tok}")
         scanner.scan
    }
    List()
  }
}
