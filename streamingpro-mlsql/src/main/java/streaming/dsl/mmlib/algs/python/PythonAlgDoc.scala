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

package streaming.dsl.mmlib.algs.python

import streaming.dsl.mmlib.{Doc, MarkDownDoc}

object PythonAlgDoc {
  def doc = {
    Doc(MarkDownDoc,
      s"""
         |
         |Requirements:
         |
         |1. Conda is installed in your cluster.
         |2. The user who runs StreamingPro cluster has the permission to read/write `/tmp/__mlsql__`.
         |
         |Suppose you run StreamingPro/MLSQL with user named `mlsql`.
         |Conda should be installed by `mlsql` and `mlsql` have the permission to read/write `/tmp/__mlsql__`.
         |
         |You can get code example by:
         |
         |```
         |load modelExample.`PythonAlg` as output;
         |```
         |
         |Actually, this doc is also can be get by this command.
         |
         |If you wanna know what params the PythonAlg have, please use the command following:
         |
         |```
         |load modelParam.`PythonAlg` as output;
         |```
         |
     """.stripMargin)
  }
}
