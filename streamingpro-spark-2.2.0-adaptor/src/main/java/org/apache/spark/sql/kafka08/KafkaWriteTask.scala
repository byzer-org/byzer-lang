package org.apache.spark.sql.kafka08

import java.util.Properties
import java.{util => ju}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
  * Created by allwefantasy on 3/7/2017.
  */
class KafkaWriteTask(
                                       producerConfiguration: ju.Map[String, Object],
                                       inputSchema: Seq[Attribute],
                                       topic: Option[String]) {
  // used to synchronize with Kafka callbacks
  @volatile private var failedWrite: Exception = null
  private val projection = createProjection
  private var producer: Producer[Array[Byte], Array[Byte]] = _

  /**
    * Writes key value data out to topics.
    */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    val p = new Properties()
    p.putAll(producerConfiguration)
    producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(p))
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      val projectedRow = projection(currentRow)
      val topic = projectedRow.getUTF8String(0)
      val key = projectedRow.getBinary(1)
      val value = projectedRow.getBinary(2)
      if (topic == null) {
        throw new NullPointerException(s"null topic present in the data. Use the ")
      }
      try {
        val record = new KeyedMessage[Array[Byte], Array[Byte]](topic.toString, key, value)
        producer.send(record)
      } catch {
        case e: Exception =>
          failedWrite = e
      }

    }
  }

  def close(): Unit = {
    if (producer != null) {
      checkForErrors
      producer.close()
      checkForErrors
      producer = null
    }
  }

  private def createProjection: UnsafeProjection = {
    val topicExpression = topic.map(Literal(_)).orElse {
      inputSchema.find(_.name == KafkaWriter.TOPIC_ATTRIBUTE_NAME)
    }.getOrElse {
      throw new IllegalStateException(s"topic option required when no " +
        s"'${KafkaWriter.TOPIC_ATTRIBUTE_NAME}' attribute is present")
    }
    topicExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t. ${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
          s"must be a ${StringType}")
    }
    val keyExpression = inputSchema.find(_.name == KafkaWriter.KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.KEY_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t")
    }
    val valueExpression = inputSchema
      .find(_.name == KafkaWriter.VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException(s"Required attribute " +
        s"'${KafkaWriter.VALUE_ATTRIBUTE_NAME}' not found")
    )
    valueExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.VALUE_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t")
    }
    UnsafeProjection.create(
      Seq(topicExpression, Cast(keyExpression, BinaryType),
        Cast(valueExpression, BinaryType)), inputSchema)
  }

  private def checkForErrors: Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }
}
