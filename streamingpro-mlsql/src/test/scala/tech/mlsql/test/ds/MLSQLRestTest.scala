package tech.mlsql.test.ds

import org.apache.http.client.fluent.{Request, Response}
import org.apache.http.entity.BasicHttpEntity
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.apache.http.{HttpResponse, ProtocolVersion}
import org.apache.spark.streaming.SparkOperationUtil
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{anyList, anyString, argThat, endsWith}
import org.mockito.Mockito.{mock, mockStatic, when}
import org.mockito.{ArgumentMatcher, ArgumentMatchers, MockedStatic}
import org.scalatest.funsuite.AnyFunSuite
import streaming.core.BasicMLSQLConfig
import streaming.core.datasource.DataSourceConfig
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.datasource.impl.MLSQLRest
import tech.mlsql.job.JobManager
import tech.mlsql.runtime.plugins.request_cleaner.RequestCleanerManager
import tech.mlsql.tool.HDFSOperatorV2

import java.io.ByteArrayInputStream


/**
 * 26/2/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLRestTest extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with Logging {

  def buildResp(str: String, status: Int = 200) = {
    val httpResp: HttpResponse = new BasicHttpResponse(new BasicStatusLine(
      new ProtocolVersion("HTTP", 1, 1), status, ""))
    val entity = new BasicHttpEntity()
    entity.setContent(new ByteArrayInputStream(str.getBytes))
    httpResp.setEntity(entity)
    httpResp
  }

  def mockStaticRequest = {
    val reqStatic = mockStatic(classOf[Request])
    val reqMock = mock(classOf[Request])
    // Get
    reqStatic.when(new MockedStatic.Verification() {
      override def apply(): Unit = {
        Request.Get(anyString)
      }
    }).thenReturn(reqMock)
    // Post json
    reqStatic.when(new MockedStatic.Verification() {
      override def apply(): Unit = {
        Request.Post(anyString)
      }
    }).thenReturn(reqMock)

    when(reqMock.bodyString(anyString, ArgumentMatchers.any())).thenReturn(reqMock)
    when(reqMock.bodyForm(anyList(), ArgumentMatchers.any())).thenReturn(reqMock)
    (reqStatic, reqMock)
  }

  test("Temp data should be cleaned if config.enableRequestCleaner is true") {
    withBatchContext(setupBatchContext(batchParamsWithoutHive, null)) { runtime: SparkRuntime =>
      // Init JobManager so that it executes script
      JobManager.init(runtime.sparkSession, initialDelay = 2 , checkTimeInterval = 3)
      val (reqStatic, _, _) = mockOffsetPaginationRequest()

      tryWithResource(reqStatic) {
        _ => {
          val script = """
                         |LOAD Rest.`http://www.byzer.org/?range=10`
                         |WHERE `config.connect-timeout`="10s"
                         |AND `config.page.next`= "http://www.byzer.org/index={0}"
                         |AND `config.method`="get"
                         |AND `config.page.retry`="1"
                         |AND `config.page.values`="offset:0,1"
                         |AND `header.content-type`="application/json"
                         |ANd `config.debug`="true"
                         |AND `config.enableRequestCleaner`="true"
                         |AS data_1;
                         |
                         |LOAD Rest.`http://www.byzer.org/?range=10`
                         |WHERE `config.connect-timeout`="10s"
                         |AND `config.page.next`= "http://www.byzer.org/index={0}"
                         |AND `config.method`="get"
                         |AND `config.page.retry`="1"
                         |AND `config.page.values`="offset:0,1"
                         |AND `header.content-type`="application/json"
                         |ANd `config.debug`="true"
                         |AND `config.enableRequestCleaner`="true"
                         |AS data_2;
                         |""".stripMargin

          val params = Map( "__PARAMS__" ->  """{"async":"true"}""")
          executeCode(runtime, script, params)
          val context = ScriptSQLExec.context()
          // Mark job finished
          context.execListener.addEnv("__MarkAsyncRunFinish__", "true")
          // Clean temp data
          RequestCleanerManager.call()

          // Assert that temp directories are deleted
          context.execListener.env().get(classOf[MLSQLRest].getName) match {
            case Some(dirs) =>
              dirs.split(",").foreach( d => assert( ! HDFSOperatorV2.fileExists(d)) )
            case None => throw new RuntimeException(s"Failed to find temp directory owner ${context.owner}")
          }
        }
      }
    }
  }

  test("auto-increment page strategy stop with equals") {

    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {

        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":"200","content":"ok"}
              |""".stripMargin)).
          thenReturn(buildResp("{\"code\":\"200\",\"content\":\"ok\"}")).
          thenReturn(buildResp("{\"code\":\"200\",\"content\":\"fail\"}"))

        when(reqMock.execute()).thenReturn(responseMock)
        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              "config.page.next" -> "http://www.byzer.org/list?{0}",
              "config.page.values" -> "auto-increment:0",
              "config.page.stop" -> "equals:$.content,fail",
              "config.page.limit" -> "4",
              "config.debug" -> "true"
            ), Option(session.emptyDataFrame)
          ))
          assertEquals(3, res.collect().length, "Page three times should have three rows")
        }
      }

    }

  }

  val FIST_PAGE_URL_SUFFIX = "range=10"
  val SUBSEQUENT_PAGE_URL_SUFFIX_PATTERN = """index=[0-9]+""".r
  /**
   * Mocks offset pagination request and response. Different responses are
   * mocked for first and subsequent page requests.
   * Please see OffsetPageStrategy
   * for offset logic.
   * @return
   */
  def mockOffsetPaginationRequest(): (MockedStatic[Request], Seq[Request], Seq[Response]) = {
    // Request's static mock
    val reqStatic = mockStatic(classOf[Request])
    val firstPageReqMock = mock(classOf[Request])

    // Mock first page request, whose url ends with "range=10
    reqStatic.when(new MockedStatic.Verification() {
      override def apply(): Unit = {
        Request.Get( endsWith(FIST_PAGE_URL_SUFFIX) )
      }
    }).thenReturn(firstPageReqMock)
    // Mock first page response -- http status 200 OK
    val fistPageRespMock = mock(classOf[Response])
    when(firstPageReqMock.execute()).thenReturn( fistPageRespMock )
    when(fistPageRespMock.returnResponse()).
      thenReturn(buildResp(
        """
          |{"code":"200","content":"ok"}
          |""".stripMargin))

    // Mock subsequent page request
    val subsequentPageReqMock = mock(classOf[Request])
    reqStatic.when(new MockedStatic.Verification() {
      override def apply(): Unit = {
        Request.Get( argThat( new ArgumentMatcher[String] {
          override def matches(argument: String): Boolean = {
            if( argument == null || argument.isEmpty) return false
            argument.split("/").last match {
              case SUBSEQUENT_PAGE_URL_SUFFIX_PATTERN(_*) => true
              case _ => false
            }
          }
        } ))
      }
    }).thenReturn(subsequentPageReqMock)
    // Mock subsequent page response http status code 204
    val subsequentPageRespMock = mock(classOf[Response])
    when( subsequentPageReqMock.execute()).thenReturn(subsequentPageRespMock)
    when( subsequentPageRespMock.returnResponse()).thenReturn(
      buildResp("""{"code":"204","content":"no more content"}""", 204))

    (reqStatic, Seq( firstPageReqMock , subsequentPageReqMock ), Seq(fistPageRespMock, subsequentPageRespMock))
  }

  test("offset page strategy should stop with http status 204") {

    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val (reqStatic, _, _) = mockOffsetPaginationRequest()

      tryWithResource(reqStatic) {
        _ => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/?range=10", Map(
              "config.method" -> "get",
              // set the retry to 1 means no retry. If this value is not 1, then the
              // retry mechanism will consume the mock response
              "config.page.retry" -> "1",
              "config.page.next" -> "http://www.byzer.org/index={0}",
              "config.page.values" -> "offset:0,1",
              "config.page.stop" -> "equals:$.content,fail",
              "config.page.limit" -> "4",
              "config.debug" -> "true"
            ), Option(session.emptyDataFrame)
          ))
          val rows = res.collect()

          assertEquals(1, rows.length)
          assertEquals(rows.head.getInt(1), 200)
        }
      }
    }
  }

  test("auto-increment page strategy stop with sizeZero") {

    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {

        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":"200","content":[{"title":"wow"}]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":"200","content":[{"title":"wow"}]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":"200","content":[]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":"200","content":[]}
              |""".stripMargin))

        when(reqMock.execute()).thenReturn(responseMock)
        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              "config.page.next" -> "http://www.byzer.org/list?{0}",
              "config.page.values" -> "auto-increment:0",
              "config.page.stop" -> "sizeZero:$.content",
              "config.page.limit" -> "4",
              "config.debug" -> "true"
            ), Option(session.emptyDataFrame)
          ))
          val rows = res.collect()
          rows.foreach(println(_))
          // the last one will also been append in the result with empty value
          assertEquals(3, rows.length)
        }
      }

    }

  }

  test("auto-increment page strategy stop with equals int value") {

    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {

        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":200,"content":[{"title":"wow"}]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":200,"content":[{"title":"wow"}]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":400,"content":[]}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":200,"content":[]}
              |""".stripMargin))

        when(reqMock.execute()).thenReturn(responseMock)
        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              "config.page.next" -> "http://www.byzer.org/list?{0}",
              "config.page.values" -> "auto-increment:0",
              "config.page.stop" -> "equals:$.code,400",
              "config.page.limit" -> "4",
              "config.debug" -> "true"
            ), Option(session.emptyDataFrame)
          ))
          val rows = res.collect()
          rows.foreach(println(_))
          // the last one will also been append in the result with empty value
          assertEquals(3, rows.length)
        }
      }

    }

  }

  test( "Default page strategy should stop if page.limit is reached") {
    withBatchContext( setupBatchContext(batchParams, null)) { runtime : SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {
        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":200,"data":{"page_token":"1"}}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":200,"data":{"page_token":"2"}}
              |""".stripMargin)).
          thenReturn(buildResp(
            """
              |{"code":200,"data":{"page_token":"3"}}
              |""".stripMargin))
        when(reqMock.execute()).thenReturn(responseMock)

        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              "config.page.retry" -> "2",
              "config.debug" -> "true",
              "config.page.next" -> "https://byzer.org/?page_token={0}",
              "config.page.values" -> "$.data.page_token",
              "config.page.limit"-> "3"
            ), Option(session.emptyDataFrame)
          ))
          val rows = res.collect()
          assert( rows.size == 3 )
        }
      }
    }
  }


  test("Non-pagination should try at most 2 times") {
    withBatchContext( setupBatchContext(batchParams, null)) { runtime : SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {
        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":200,"content":[{"title":"wow"}]}
              |""".stripMargin, status = 400))

        when(reqMock.execute()).thenReturn(responseMock)

        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              "config.page.retry" -> "1",
              "config.debug" -> "true"
            ), Option(session.emptyDataFrame)
          ))
          val row = res.collect().head
          assert(row.getInt(1) == 400)
        }
      }
    }
  }

  test("MLSQLRest should return error message if remote is not reachable") {
    withBatchContext( setupBatchContext(batchParams, null)) { runtime : SparkRuntime =>
      autoGenerateContext(runtime)
      val rest = new MLSQLRest()
      val session = ScriptSQLExec.context().execListener.sparkSession
      val res = rest.load(session.read, DataSourceConfig(
        "http://www.akjxcvzoixcv.dfasoidfkast", Map(
          "config.page.retry" -> "1",
          "config.debug" -> "true"
        ), Option(session.emptyDataFrame)
      ))
      val row = res.collect().head
      assert(row.getInt(1) == 0)
    }
  }

  test("unsupported content-type yields 415 status code") {
    withBatchContext( setupBatchContext(batchParams, null)) { runtime : SparkRuntime =>
      autoGenerateContext(runtime)
      val rest = new MLSQLRest()
      val session = ScriptSQLExec.context().execListener.sparkSession
      val res = rest.load(session.read, DataSourceConfig(
        "http://www.byzer.org/list", Map(
          "config.page.retry" -> "1",
          "config.debug" -> "true",
          "config.method" -> "put",
          "header.content-type" -> "application/unsupported"
        ), Option(session.emptyDataFrame)
      ))
      val row = res.collect().head
      assert(row.getInt(1) == 415)
    }
  }

  test("unsupported http method yields 405 status code") {
    withBatchContext( setupBatchContext(batchParams, null)) { runtime : SparkRuntime =>
      autoGenerateContext(runtime)
      val rest = new MLSQLRest()
      val session = ScriptSQLExec.context().execListener.sparkSession
      val res = rest.load(session.read, DataSourceConfig(
        "http://www.byzer.org/list", Map(
          "config.page.retry" -> "1",
          "config.debug" -> "true",
          "config.method" -> "unknown"
        ), Option(session.emptyDataFrame)
      ))
      val row = res.collect().head
      assert(row.getInt(1) == 405)
    }
  }

}
