package streaming.tensorflow

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.tensorflow.{SavedModelBundle, Tensor}

/**
  * Created by allwefantasy on 28/1/2018.
  */
object TFModelLoader {

  val loader = new CacheLoader[String, SavedModelBundle]() {
    override def load(key: String): SavedModelBundle = {
      _load(key)
    }
  }
  val cache = CacheBuilder.newBuilder().
    maximumSize(10).
    expireAfterAccess(5, TimeUnit.MINUTES).
    removalListener(new RemovalListener[String, SavedModelBundle]() {
      override def onRemoval(notification: RemovalNotification[String, SavedModelBundle]): Unit = {
        notification.getValue.close()
      }
    }).build[String, SavedModelBundle](loader)


  val map = new java.util.concurrent.ConcurrentHashMap[String, SavedModelBundle]()
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
        fs.copyToLocalFile(new Path(modelPath),
          new Path(localPath))

        val smb = SavedModelBundle.load(localPath, "serve")
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
      sm.close()
    }
  }

}

object TFModelPredictor {
  def run_double(modelBundle: SavedModelBundle, inputName: String, outputName: String, outputSize: Int, data: Array[Array[Double]]) = {
    val inputTensor = Tensor.create(data)
    val res = modelBundle.session().runner().feed(inputName, inputTensor).fetch(outputName).run().get(0)
    val resCopy = res.copyTo(Array.ofDim[Double](1, outputSize))
    res.close()
    inputTensor.close()
    resCopy(0)
  }

  def run_float(modelBundle: SavedModelBundle, inputName: String, outputName: String, outputSize: Int, data: Array[Array[Float]]) = {
    val inputTensor = Tensor.create(data)
    val res = modelBundle.session().runner().feed(inputName, inputTensor).fetch(outputName).run().get(0)
    val resCopy = res.copyTo(Array.ofDim[Float](1, outputSize))
    res.close()
    inputTensor.close()
    resCopy(0)
  }
}
