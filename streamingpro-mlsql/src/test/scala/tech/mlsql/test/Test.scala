package tech.mlsql.test

import streaming.common.shell.ShellCommand

/**
  * 2019-08-07 WilliamZhu(allwefantasy@gmail.com)
  */
object Test {
  def main(args: Array[String]): Unit = {
    //println(ShellCommand.execCmd("/anaconda3/bin/conda"))
    val res = os.proc("/anaconda3/bin/conda".split("\\s+")).call()
    println(res.out)
  }
}
