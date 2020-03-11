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

package streaming.dsl.template

import org.joda.time.DateTime
import tech.mlsql.common.utils.evaluate.RenderEngine
import tech.mlsql.template.SQLSnippetTemplate


/**
 * Created by allwefantasy on 29/3/2018.
 */
object TemplateMerge {

  def merge(sql: String, root: Map[String, String]) = {

    val dformat = "yyyy-MM-dd"
    //2018-03-24
    val predified_variables = Map[String, String](
      "yesterday" -> DateTime.now().minusDays(1).toString(dformat),
      "today" -> DateTime.now().toString(dformat),
      "tomorrow" -> DateTime.now().plusDays(1).toString(dformat),
      "theDayBeforeYesterday" -> DateTime.now().minusDays(2).toString(dformat)
    )
    val newRoot = Map("date" -> new DateTime(), "template" -> new SQLSnippetTemplate()) ++ predified_variables ++ root

    RenderEngine.render(sql, newRoot)
  }
}
