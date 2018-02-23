package streaming.dl4j

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.Vectors
import org.deeplearning4j.nn.api.Model
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.factory.Nd4j

/**
  * Created by allwefantasy on 28/1/2018.
  */
object DL4JModelLoader {

  val loader = new CacheLoader[String, Model]() {
    override def load(key: String): Model = {
      _load(key)
    }
  }
  val cache = CacheBuilder.newBuilder().
    maximumSize(10).
    expireAfterAccess(5, TimeUnit.MINUTES).
    removalListener(new RemovalListener[String, Model]() {
      override def onRemoval(notification: RemovalNotification[String, Model]): Unit = {
        //notification.getValue
      }
    }).build[String, Model](loader)


  val map = new java.util.concurrent.ConcurrentHashMap[String, Model]()
  val loading_status_map = new java.util.concurrent.ConcurrentHashMap[String, Int]()

  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }

  def load(modelPath: String) = {
    cache.get(modelPath)
  }

  def _load(modelPath: String) = synchronized {
    var count = 0
    val count_upper_bound = 10
    //if take too much time to load eg. 50s,then maybe just return null
    if (map.containsKey(modelPath) || loading_status_map.containsKey(modelPath)) {
      while (loading_status_map.containsKey(modelPath) && count < count_upper_bound) {
        Thread.sleep(5000)
        count += 1
      }
      map.get(modelPath)
    } else {
      loading_status_map.put(modelPath, 1)
      try {
        val localPath = s"/tmp/${md5Hash(modelPath)}"
        FileUtils.deleteDirectory(new File(localPath))
        val fs = FileSystem.get(new Configuration())
        fs.copyToLocalFile(new Path(modelPath + "/dl4j.model"),
          new Path(localPath + "/dl4j.model"))
        val smb = ModelSerializer.restoreMultiLayerNetwork(localPath + "/dl4j.model")
        map.put(modelPath, smb)
      } finally {
        loading_status_map.remove(modelPath)
      }
      map.get(modelPath)
    }
  }

  def close(modelPath: String) = {
    if (map.containsKey(modelPath)) {
      val sm = map.remove(modelPath)
      try {
        FileUtils.deleteDirectory(new File(s"/tmp/${md5Hash(modelPath)}"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

}

object DL4JModelPredictor {
  def run_double(modelBundle: MultiLayerNetwork, vec: org.apache.spark.ml.linalg.Vector) = {
    val array = modelBundle.feedForward(Nd4j.create(vec.toArray), false).get(0).data().asDouble()
    Vectors.dense(array)
  }
}
