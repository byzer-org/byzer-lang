package org.apache.spark.sql.delta.sources.mysql.binlog

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.Executors

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.delta.sources.ExecutorInternalBinlogConsumer
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.{SparkEnv, SparkException, TaskContext}


object SocketServerInExecutor extends Logging {

  private val binlogServerHolder = new java.util.concurrent.ConcurrentHashMap[MySQLBinlogServer, BinLogSocketServerInExecutor]()
  val threadPool = Executors.newFixedThreadPool(100)

  def addNewBinlogServer(a: MySQLBinlogServer, b: BinLogSocketServerInExecutor) = {
    binlogServerHolder.put(a, b)
  }

  def setupOneConnectionServer(threadName: String)
                              (func: Socket => Unit): (ServerSocket, String, Int) = {
    val host = SparkEnv.get.rpcEnv.address.host
    val serverSocket: ServerSocket = new ServerSocket(0, 1, InetAddress.getByName(host))
    // Close the socket if no connection in 1 hour seconds
    serverSocket.setSoTimeout(1000 * 60 * 60)
    new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        var sock: Socket = null
        try {
          sock = serverSocket.accept()
          func(sock)
        } finally {
          JavaUtils.closeQuietly(serverSocket)
          JavaUtils.closeQuietly(sock)
        }
      }
    }.start()

    (serverSocket, host, serverSocket.getLocalPort)
  }

  def setupMultiConnectionServer(threadName: String)
                                (func: Socket => Unit): (ServerSocket, String, Int) = {


    val host = SparkEnv.get.rpcEnv.address.host
    val serverSocket: ServerSocket = new ServerSocket(0, 1, InetAddress.getByName(host))
    // Close the socket if no connection in 1 hour seconds
    serverSocket.setSoTimeout(1000 * 60 * 60)

    new Thread(threadName) {
      setDaemon(true)

      override def run(): Unit = {
        try {
          while (true) {
            try {
              val socket = serverSocket.accept()
              threadPool.submit(new Runnable {
                override def run(): Unit = {
                  try {
                    func(socket)
                  } finally {
                    JavaUtils.closeQuietly(socket)
                  }
                }
              })
            } catch {
              case e: Exception => logError("", e)
            }

          }
          JavaUtils.closeQuietly(serverSocket)
        }
        catch {
          case e: Exception => logError("", e)
        }

      }
    }.start()

    (serverSocket, host, serverSocket.getLocalPort)
  }
}

abstract class SocketServerInExecutor[T](threadName: String) {

  val (server, host, port) = SocketServerInExecutor.setupMultiConnectionServer(threadName) { sock =>
    handleConnection(sock)
  }

  def handleConnection(sock: Socket): T
}

trait BinLogSocketServerSerDer {
  def readRequest(in: InputStream) = {
    val dIn = new DataInputStream(in)
    while (dIn.available() <= 0) {
      Thread.sleep(10)
    }
    val request = JsonUtils.fromJson[BinlogSocketRequest](dIn.readUTF()).unwrap
    println("readRequest:" + request)
    request
  }

  def sendRequest(out: OutputStream, request: Request) = {
    println("sendRequest:" + request.json)
    val dOut = new DataOutputStream(out)
    dOut.writeUTF(request.json)
    dOut.flush()
  }

  def sendResponse(out: OutputStream, response: Response) = {
    println("sendResponse:" + response.json)
    val dOut = new DataOutputStream(out)
    dOut.writeUTF(response.json)
    dOut.flush()
  }

  def readResponse(in: InputStream) = {
    val dIn = new DataInputStream(in)
    while (dIn.available() <= 0) {
      Thread.sleep(10)
    }
    val response = JsonUtils.fromJson[BinlogSocketResponse](dIn.readUTF()).unwrap
    println("readResponse:" + response)
    response
  }
}

object ExecutorBinlogServerConsumerCache extends Logging {

  private case class CacheKey(host: String, port: Int)

  private lazy val cache = {
    val conf = SparkEnv.get.conf
    val capacity = conf.getInt("spark.sql.mlsql.binlog.capacity", 64)
    new java.util.LinkedHashMap[CacheKey, ExecutorInternalBinlogConsumer](capacity, 0.75f, true) {
      override def removeEldestEntry(
                                      entry: java.util.Map.Entry[CacheKey, ExecutorInternalBinlogConsumer]): Boolean = {

        // Try to remove the least-used entry if its currently not in use.
        //
        // If you cannot remove it, then the cache will keep growing. In the worst case,
        // the cache will grow to the max number of concurrent tasks that can run in the executor,
        // (that is, number of tasks slots) after which it will never reduce. This is unlikely to
        // be a serious problem because an executor with more than 64 (default) tasks slots is
        // likely running on a beefy machine that can handle a large number of simultaneously
        // active consumers.

        if (!entry.getValue.inUse && this.size > capacity) {
          logWarning(
            s"KafkaConsumer cache hitting max capacity of $capacity, " +
              s"removing consumer for ${entry.getKey}")
          try {
            entry.getValue.close
          } catch {
            case e: SparkException =>
              logError(s"Error closing earliest Kafka consumer for ${entry.getKey}", e)
          }
          true
        } else {
          false
        }
      }
    }
  }

  def acquire(executorBinlogServer: ExecutorBinlogServer): ExecutorInternalBinlogConsumer = synchronized {
    val key = new CacheKey(executorBinlogServer.host, executorBinlogServer.port)
    val existingInternalConsumer = cache.get(key)

    lazy val newInternalConsumer = new ExecutorInternalBinlogConsumer(executorBinlogServer)

    if (TaskContext.get != null && TaskContext.get.attemptNumber >= 1) {
      // If this is reattempt at running the task, then invalidate cached consumer if any and
      // start with a new one.
      if (existingInternalConsumer != null) {
        // Consumer exists in cache. If its in use, mark it for closing later, or close it now.
        if (existingInternalConsumer.inUse) {
          existingInternalConsumer.markedForClose = true
        } else {
          existingInternalConsumer.close
        }
      }
      cache.remove(key) // Invalidate the cache in any case
      newInternalConsumer

    } else if (existingInternalConsumer == null) {
      // If consumer is not already cached, then put a new in the cache and return it
      cache.put(key, newInternalConsumer)
      newInternalConsumer.inUse = true
      newInternalConsumer

    } else if (existingInternalConsumer.inUse) {
      // If consumer is already cached but is currently in use, then return a new consumer
      newInternalConsumer

    } else {
      // If consumer is already cached and is currently not in use, then return that consumer
      existingInternalConsumer.inUse = true
      existingInternalConsumer
    }
  }

  private def release(intConsumer: ExecutorInternalBinlogConsumer): Unit = {
    synchronized {

      // Clear the consumer from the cache if this is indeed the consumer present in the cache
      val key = new CacheKey(intConsumer.executorBinlogServer.host,
        intConsumer.executorBinlogServer.port)

      val cachedIntConsumer = cache.get(key)
      if (intConsumer.eq(cachedIntConsumer)) {
        // The released consumer is the same object as the cached one.
        if (intConsumer.markedForClose) {
          intConsumer.close
          cache.remove(key)
        } else {
          intConsumer.inUse = false
        }
      } else {
        // The released consumer is either not the same one as in the cache, or not in the cache
        // at all. This may happen if the cache was invalidate while this consumer was being used.
        // Just close this consumer.
        intConsumer.close
        logInfo(s"Released a supposedly cached consumer that was not found in the cache")
      }
    }
  }
}
