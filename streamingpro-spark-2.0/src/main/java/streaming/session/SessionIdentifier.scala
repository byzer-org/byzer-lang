package streaming.session

/**
  * Created by allwefantasy on 4/6/2018.
  */

import java.nio.ByteBuffer
import java.util.UUID

case class SessionIdentifier(publicId: UUID, secretId: UUID) {

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
    if (!obj.isInstanceOf[SessionIdentifier]) return false

    val other = obj.asInstanceOf[SessionIdentifier]
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