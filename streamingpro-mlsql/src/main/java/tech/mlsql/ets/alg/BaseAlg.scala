package tech.mlsql.ets.alg

import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.path.PathFun


/**
  * 2019-05-15 WilliamZhu(allwefantasy@gmail.com)
  */
trait BaseAlg {
  def isModelPath(path: String) = {
    def splitPath(item: String) = {
      item.split("/").last
    }

    val paths = HDFSOperator.listFiles(path).map(file => PathFun(path).add(file.getPath.getName).toPath)

    !paths.isEmpty && (paths.filter(splitPath(_).startsWith("_model_")).size > 0 ||
      (paths.filter(splitPath(_) == "model").size > 0 &&
        paths.filter(splitPath(_) == "meta").size > 0))
  }
}
