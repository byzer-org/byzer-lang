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

/**
 * Represents the result of executing a command.
 */
object ContainerExecResultBytes {
  def of(exitCode: Long, stdout: Array[Byte], stderr: Array[Byte]) = new ContainerExecResultBytes(exitCode, stdout, stderr)
}

class ContainerExecResultBytes private(val exitCode: Long, val stdout: Array[Byte], val stderr: Array[Byte]) {
  override def toString = "ContainerExecResultBytes(exitCode=" + this.getExitCode + ", stdout=" + java.util.Arrays.toString(this.getStdout) + ", stderr=" + java.util.Arrays.toString(this.getStderr) + ")"

  def getExitCode: Long = this.exitCode

  def getStdout: Array[Byte] = this.stdout

  def getStderr: Array[Byte] = this.stderr
}