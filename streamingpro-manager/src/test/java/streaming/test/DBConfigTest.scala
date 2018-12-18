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

package streaming.test

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.scalatest.{FlatSpec, Matchers}
import streaming.common.ParamsUtil
import streaming.db.{DB, ManagerConfiguration}

/**
  * Created by allwefantasy on 16/7/2017.
  */
class DBConfigTest extends FlatSpec with Matchers {
  "loading order" should "classpath  command" in {
    ManagerConfiguration.config = new ParamsUtil(Array(
      "-jdbcPath", "classpath:///jdbc.properties",
      "-jdbc.url", "c1",
      "-jdbc.userName", "c2"))

    val item = DB.parseConfig
    assume(item("url") == "c1")
    assume(item("userName") == "c2")
    assume(item("password") != "c")
  }
  "loading order" should "local path command" in {

    val content = "url=a\nuserName=b\npassword=c"
    val path = new File("/tmp/" + System.currentTimeMillis())
    Files.write(content, path, Charset.forName("utf-8"))

    ManagerConfiguration.config = new ParamsUtil(Array(
      "-jdbcPath", path.getPath,
      "-jdbc.url", "c1",
      "-jdbc.userName", "c2"))

    val item = DB.parseConfig
    assume(item("url") == "c1")
    assume(item("userName") == "c2")
    assume(item("password") == "c")
  }
}
