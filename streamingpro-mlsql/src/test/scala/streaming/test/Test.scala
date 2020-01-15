package streaming.test

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, Future}

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.internal.Logging
import streaming.dsl.DBMappingKey
import tech.mlsql.common.utils.lang.sc.ScalaReflect

/**
  * 2019-03-05 WilliamZhu(allwefantasy@gmail.com)
  */
object Test {
  def main(args: Array[String]): Unit = {
    val item = new AtomicInteger(0)
    ProfileUtils.profileMuti(() => {
      val start = System.currentTimeMillis()

      val userName = UUID.randomUUID().toString

      Request.Post("http://127.0.0.1:9003/run/script")
        .connectTimeout(60 * 1000)
        .socketTimeout(10 * 60 * 1000).bodyForm(Form.form()
        .add("defaultPathPrefix", "/Users/mlsql/" + userName)
        .add("skipAuth", "false")
        .add("skipGrammarValidate", "false")
        .add("context.__auth_secret__", "729ec639-e309-4b10-b1b4-6ebe8ad7ce72")
        .add("context.__auth_client__", "streaming.dsl.auth.meta.client.MLSQLConsoleClient")
        .add("context.__auth_server_url__", "http://127.0.0.1:9002/api_v1/table/auth")
        .add("sessionPerUser", "true")
        .add("sessionPerRequest", "true")
        .add("owner", userName)
        .add("jobName", userName)
        .add("sql",
          """
            | set data='''
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |''';
            |
            |-- load data as table
            |load jsonStr.`data` as datasource;
          """.stripMargin)
        .build())
        .execute().returnContent().asString()

//      Request.Post("http://127.0.0.1:9003/user/logout")
//        .connectTimeout(60 * 1000)
//        .socketTimeout(10 * 60 * 1000).bodyForm(Form.form()
//        .add("defaultPathPrefix", "/Users/mlsql/" + userName)
//        .add("skipAuth", "false")
//        .add("skipGrammarValidate", "false")
//        .add("context.__auth_secret__", "729ec639-e309-4b10-b1b4-6ebe8ad7ce72")
//        .add("context.__auth_client__", "streaming.dsl.auth.meta.client.MLSQLConsoleClient")
//        .add("context.__auth_server_url__", "http://127.0.0.1:9002/api_v1/table/auth")
//        .add("sessionPerUser", "true")
//        .add("owner", userName)
//        .add("jobName", userName)
//        .add("sql",
//          """
//            | set data='''
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
//            |{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
//            |''';
//            |
//            |-- load data as table
//            |load jsonStr.`data` as datasource;
//          """.stripMargin)
//        .build())
//        .execute().returnContent().asString()
//
//      println(item.addAndGet(1))

      val end = System.currentTimeMillis()
      end - start
    }, 100000, 1)
  }
}

object ProfileUtils extends Logging {

  def profileMuti(f: () => Long, times: Int, threads: Int): Unit = {
    val executorService = Executors.newFixedThreadPool(threads)

    val b: IndexedSeq[Future[Long]] = 1.to(times).map(i => {
      executorService.submit(new Callable[Long] {
        override def call(): Long = {
          try {
            val t = f.apply()
            t
          } catch {
            case e: Exception => {}
              log.error("opps", e)
              0
          }
        }
      })
    })
    val t = b.map(task => task.get())
    println("meanTime " + mean(removeOuterlier(t)))
    executorService.shutdownNow()
  }

  import Numeric.Implicits._

  def mean[T: Numeric](xs: Iterable[T]): Double = {
    xs.sum.toDouble() / xs.size
  }

  def removeOuterlier[T: Numeric](xs: Iterable[T]): Iterable[T] = {
    val std = stdV(xs)
    val m = mean(xs)
    xs.filter(p => Math.abs(p.toDouble() - m) < 1 * std)
  }

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdV[T: Numeric](xs: Iterable[T]): Double = {
    Math.sqrt(variance(xs))
  }
}

object Wow {
  def main(args: Array[String]): Unit = {
    val dbmapping = ScalaReflect.fromObjectStr("streaming.dsl.ConnectMeta").field("dbMapping").invoke().asInstanceOf[ConcurrentHashMap[DBMappingKey, Map[String, String]]]
    println(dbmapping)
  }
}