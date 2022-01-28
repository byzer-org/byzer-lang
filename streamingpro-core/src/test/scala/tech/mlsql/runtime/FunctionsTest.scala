/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.mlsql.runtime

import org.apache.http.client.fluent.{Request, Response}
import org.apache.http.entity.BasicHttpEntity
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.apache.http.{HttpResponse, ProtocolVersion}
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.ArgumentMatchers.{anyList, anyString}
import org.mockito.Mockito.{mock, mockStatic, when}
import org.mockito.{ArgumentMatchers, MockedStatic}
import org.scalatest.FunSuite
import streaming.dsl.ScriptSQLExec
import tech.mlsql.crawler.udf.FunctionsUtils

import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import scala.language.reflectiveCalls

/**
 * 22/01/2022 hellozepp(lisheng.zhanglin@163.com)
 */

class FunctionsTest extends FunSuite {

  def initRest(reqStatic: MockedStatic[Request]): Unit = {
    val reqMock = mock(classOf[Request])
    val httpResp: HttpResponse = new BasicHttpResponse(new BasicStatusLine(
      new ProtocolVersion("HTTP", 1, 1), 200, ""))
    val entity = new BasicHttpEntity()
    entity.setContent(new ByteArrayInputStream("{\"code\":\"200\",\"content\":\"ok\"}".getBytes()))
    httpResp.setEntity(entity)
    val responseMock = mock(classOf[Response])
    when(responseMock.returnResponse()).thenReturn(httpResp)
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
    // Post bodyForm
    when(reqMock.bodyForm(anyList(), ArgumentMatchers.any())).thenReturn(reqMock)
    ScriptSQLExec.contextGetOrForTest()
    when(reqMock.execute()).thenReturn(responseMock)
  }

  test("http get") {
    val reqStatic = mockStatic(classOf[Request])
    tryWithResource(reqStatic) {
      initRest(reqStatic)
      reqStatic => {
        val (status, content) = FunctionsUtils._http("http://www.byzer.org/home", "get",
          params = Map("foo" -> "bar", "foo1" -> "bar"), headers = Map("Content-Type" -> "application/x-www-form-urlencoded"), Map())

        println(s"status:$status, content:$content")
        assertEquals(status, 200)
        assertEquals(content, "{\"code\":\"200\",\"content\":\"ok\"}")
        // verify url concat is legal.
        reqStatic.verify(new MockedStatic.Verification() {
          override def apply(): Unit = {
            Request.Get("http://www.byzer.org/home?foo=bar&foo1=bar")
          }
        })
        // verify config set illegal of socket-timeout.
        try {
          FunctionsUtils._http("http://www.byzer.org/home", "get",
            params = Map("foo" -> "bar", "foo1" -> "bar"), Map(), config = Map("socket-timeout" -> "a"))
          throw new MLSQLException("The configuration is illegal, but no exception is displayed!")
        } catch {
          case e: Exception => println("success! e:" + e.getMessage)
        }
        // verify config set illegal of connect-timeout.
        try {
          FunctionsUtils._http("http://www.byzer.org/home", "get",
            params = Map("foo" -> "bar", "foo1" -> "bar"), Map(), config = Map("connect-timeout" -> "a"))
          throw new MLSQLException("The configuration is illegal, but no exception is displayed!")
        } catch {
          case e: Exception => println("success! e:" + e.getMessage)
        }
      }
    }
  }

  test("http post with json body") {
    val reqStatic = mockStatic(classOf[Request])
    tryWithResource(reqStatic) {
      initRest(reqStatic)
      reqStatic => {
        val (status, content) = FunctionsUtils._http("http://www.byzer.org/home", "post",
          params = Map("body" -> "{\"a\":1,\"b\":2}"), headers = Map("Content-Type" -> "application/json"), Map())

        println(s"status:$status, content:$content")
        assertEquals(status, 200)
        assertEquals(content, "{\"code\":\"200\",\"content\":\"ok\"}")
        // verify url concat is legal.
        reqStatic.verify(new MockedStatic.Verification() {
          override def apply(): Unit = {
            Request.Post("http://www.byzer.org/home")
          }
        })

        {
          val (status, content) = FunctionsUtils._http("http://www.byzer.org/home", "post",
            params = Map("foo" -> "bar", "foo1" -> "bar"), headers = Map("Content-Type" -> "application/x-www-form-urlencoded"), Map())
          println(s"status:$status, content:$content")
        }

      }
    }
  }

  def tryWithResource[A <: {def close(): Unit}, B](a: A)(f: A => B): B = {
    try f(a)
    finally {
      if (a != null) a.close()
    }
  }

}
