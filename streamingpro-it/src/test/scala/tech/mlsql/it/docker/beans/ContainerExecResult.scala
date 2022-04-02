/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *//**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package tech.mlsql.it.docker.beans

import org.junit.Assert

/**
 * Represents the result of executing a command.
 */
object ContainerExecResult {
  def of(exitCode: Long, stdout: String, stderr: String) = new ContainerExecResult(exitCode, stdout, stderr)
}

class ContainerExecResult private(val exitCode: Long, val stdout: String, val stderr: String) {
  def assertNoOutput(): Unit = {
    assertNoStdout()
    assertNoStderr()
  }

  def assertNoStdout(): Unit = Assert.assertTrue("stdout should be empty, but was '" + stdout + "'", stdout.isEmpty)

  def assertNoStderr(): Unit = Assert.assertTrue("stderr should be empty, but was '" + stderr + "'", stderr.isEmpty)

  override def toString: String = "ContainerExecResult(exitCode=" + this.getExitCode + ", stdout=" + this.getStdout + ", stderr=" + this.getStderr + ")"

  def getExitCode: Long = this.exitCode

  def getStdout: String = this.stdout

  def getStderr: String = this.stderr
}