package streaming.dl4j

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import net.csdn.common.logging.Loggers
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.Vectors
import org.deeplearning4j.nn.api.Model
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.factory.Nd4j
import streaming.tensorflow.TFModelLoader

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 28/1/2018.
  */
object DL4JModelLoader {
  val logger = Loggers.getLogger("DL4JModelLoader")
  val executor = Executors.newSingleThreadScheduledExecutor()
  val loader = new CacheLoader[String, (Model, Long)]() {
    override def load(key: String): (Model, Long) = {
      _load(key)
    }
  }
  val cache = CacheBuilder.newBuilder().
    maximumSize(10).
    expireAfterAccess(5, TimeUnit.MINUTES).
    removalListener(new RemovalListener[String, (Model, Long)]() {
      override def onRemoval(notification: RemovalNotification[String, (Model, Long)]): Unit = {
        //notification.getValue
      }
    }).build[String, (Model, Long)](loader)


  val loading_status_map = new java.util.concurrent.ConcurrentHashMap[String, Int]()

  def run = {
    executor.scheduleWithFixedDelay(new Runnable() {
      override def run(): Unit = {
        cache.asMap().map { f =>
          val fs = FileSystem.get(new Configuration())
          val currentTime = fs.getFileStatus(new Path(f._1)).getModificationTime
          val oldTime = f._2._2
          if (currentTime > oldTime) {
            logger.info(s"refresh Dl4j Model: ${f._1}")
            cache.refresh(f._1)
          }
        }
      }
    }, 30, 10, TimeUnit.SECONDS)
  }

  run

  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }

  def load(modelPath: String) = {
    cache.get(modelPath)._1
  }

  def _load(modelPath: String): (Model, Long) = synchronized {
    var count = 0
    val count_upper_bound = 10
    //if take too much time to load eg. 50s,then maybe just return null
    if (loading_status_map.containsKey(modelPath)) {
      while (loading_status_map.containsKey(modelPath) && count < count_upper_bound) {
        Thread.sleep(5000)
        count += 1
      }
      return _load(modelPath)
    } else {
      loading_status_map.put(modelPath, 1)

      try {
        val localPath = s"/tmp/${md5Hash(modelPath)}"
        FileUtils.deleteDirectory(new File(localPath))
        val fs = FileSystem.get(new Configuration())
        fs.copyToLocalFile(new Path(modelPath + "/dl4j.model"),
          new Path(localPath + "/dl4j.model"))
        val smb = ModelSerializer.restoreMultiLayerNetwork(localPath + "/dl4j.model")
        return (smb, fs.getFileStatus(new Path(localPath + "/dl4j.model")).getModificationTime)
      } finally {
        loading_status_map.remove(modelPath)
      }
    }
  }

  def close(modelPath: String) = {

  }

}

object DL4JModelPredictor {
  def run_double(modelBundle: MultiLayerNetwork, vec: org.apache.spark.ml.linalg.Vector) = {
    val array = modelBundle.feedForward(Nd4j.create(vec.toArray), false).last.data().asDouble()
    Vectors.dense(array)
  }

  def activate(modelBundle: MultiLayerNetwork, vec: org.apache.spark.ml.linalg.Vector) = {
    val vae = modelBundle.asInstanceOf[MultiLayerNetwork].getLayer(0).asInstanceOf[org.deeplearning4j.nn.layers.variational.VariationalAutoencoder]
    val array = vae.activate(Nd4j.create(vec.toArray), false).data().asDouble()
    Vectors.dense(array)
  }
}
