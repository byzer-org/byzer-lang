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

package org.apache.spark.sql.mlsql.session

/**
  * Created by allwefantasy on 4/6/2018.
  */

import java.nio.ByteBuffer
import java.util.UUID

case class OpIdentifier(publicId: UUID, secretId: UUID) {

  def this() = this(UUID.randomUUID(), UUID.randomUUID())

  def this(guid: ByteBuffer, secret: ByteBuffer) =
    this(
      if (guid == null) {
        UUID.randomUUID()
      } else {
        new UUID(guid.getLong(), guid.getLong())
      },
      if (secret == null) {
        UUID.randomUUID()
      } else {
        new UUID(secret.getLong(), secret.getLong())
      })


  def getPublicId: UUID = this.publicId

  def getSecretId: UUID = this.secretId

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (publicId == null) 0 else publicId.hashCode)
    result = prime * result + (if (secretId == null) 0 else secretId.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (!obj.isInstanceOf[OpIdentifier]) return false

    val other = obj.asInstanceOf[OpIdentifier]
    if (this eq other) return true

    if (publicId == null) {
      if (other.publicId != null) {
        return false
      }
    } else if (!(publicId == other.publicId)) {
      return false
    }

    if (secretId == null) {
      if (other.secretId != null) {
        return false
      }
    } else if (!(secretId == other.secretId)) {
      return false
    }
    true
  }

  override def toString: String = publicId.toString
}
