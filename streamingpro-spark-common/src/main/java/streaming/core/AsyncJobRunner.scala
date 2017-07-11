package streaming.core

import java.util.concurrent.Executors

import org.apache.log4j.Logger

/**
  * Created by allwefantasy on 11/7/2017.
  */
object AsyncJobRunner {
  val logger = Logger.getLogger(classOf[AsyncJobRunner])
  val executor = Executors.newFixedThreadPool(100)

  def run(f: () => Unit) = {
    executor.execute(new Runnable {
      override def run(): Unit = {
        f()
      }
    })
  }
}

class AsyncJobRunner {

}
