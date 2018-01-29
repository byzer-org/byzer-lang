package streaming.tensorflow

import java.nio.FloatBuffer

import org.tensorflow.{SavedModelBundle, Tensor}

/**
  * Created by allwefantasy on 28/1/2018.
  */
object TFModelLoader {

  val map = new java.util.concurrent.ConcurrentHashMap[String, SavedModelBundle]()
  val loading_status_map = new java.util.concurrent.ConcurrentHashMap[String, Int]()

  def load(modelPath: String) = synchronized {
    var count = 0
    val count_upper_bound = 10
    //if take too much time to load 50s ,then maybe just return null
    if (map.containsKey(modelPath) || loading_status_map.containsKey(modelPath)) {
      while (loading_status_map.containsKey(modelPath) && count < count_upper_bound) {
        Thread.sleep(5000)
        count += 1
      }
      map.get(modelPath)
    } else {
      loading_status_map.put(modelPath, 1)
      try {
        val smb = SavedModelBundle.load(modelPath, "serve")
        map.put(modelPath, smb)
      } finally {
        loading_status_map.remove(modelPath)
      }
      map.get(modelPath)
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
