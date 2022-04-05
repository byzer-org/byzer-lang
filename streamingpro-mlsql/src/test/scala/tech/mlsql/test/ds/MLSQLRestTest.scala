package tech.mlsql.test.ds

import org.apache.http.client.fluent.{Request, Response}
import org.apache.http.entity.BasicHttpEntity
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.apache.http.{HttpResponse, ProtocolVersion}
import org.apache.spark.streaming.SparkOperationUtil
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{anyList, anyString}
import org.mockito.Mockito.{mock, mockStatic, when}
import org.mockito.{ArgumentMatchers, MockedStatic}
import org.scalatest.FunSuite
import streaming.core.BasicMLSQLConfig
import streaming.core.datasource.DataSourceConfig
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.datasource.impl.MLSQLRest

import java.io.ByteArrayInputStream

/**
 * 26/2/2022 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLRestTest extends FunSuite with SparkOperationUtil with BasicMLSQLConfig with Logging {

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


  test("offset page strategy stop with wrong status") {

    withBatchContext(setupBatchContext(batchParams, null)) { runtime: SparkRuntime =>
      val (reqStatic, reqMock) = mockStaticRequest
      val responseMock = mock(classOf[Response])
      tryWithResource(reqStatic) {

        when(responseMock.returnResponse()).
          thenReturn(buildResp(
            """
              |{"code":"200","content":"ok"}
              |""".stripMargin)).
          thenReturn(buildResp("{\"code\":\"200\",\"content\":\"ok\"}", 400)).
          thenReturn(buildResp("{\"code\":\"200\",\"content\":\"fail\"}"))

        when(reqMock.execute()).thenReturn(responseMock)
        reqStatic => {
          autoGenerateContext(runtime)
          val rest = new MLSQLRest()
          val session = ScriptSQLExec.context().execListener.sparkSession
          val res = rest.load(session.read, DataSourceConfig(
            "http://www.byzer.org/list", Map(
              // set the retry to 1 means no retry. If this value is not 1, then the
              // retry mechanism will consume the mock response
              "config.page.retry" -> "1",
              "config.page.next" -> "http://www.byzer.org/list?{0}",
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
              "config.page.retry" -> "2",
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
