package streaming.dsl.mmlib.algs

/**
  * Created by allwefantasy on 22/5/2018.
  */
object MetaConst {
  def TF_IDF_PATH(path: String, col: String) = s"$path/columns/$col/tfidf"

  def WORD2VEC_PATH(path: String, col: String) = s"$path/columns/$col/word2vec"

  def WORD_INDEX_PATH(path: String, col: String) = s"$path/columns/$col/wordIndex"

  def OUTLIER_VALUE_PATH(path: String, col: String) = s"$path/columns/$col/outlierValues"

  def MIN_MAX_PATH(path: String, col: String) = s"$path/columns/$col/minMax"

  def STANDARD_SCALER_PATH(path: String, col: String) = s"$path/columns/$col/standardScaler"

  def QUANTILE_DISCRETIZAR_PATH(path: String, col: String) = s"$path/columns/$col/quantileDiscretizer"

  def PARAMS_PATH(path: String, col: String) = s"$path/params"

  def getDataPath(path: String) = {
    s"${path.stripSuffix("/")}/data"
  }

  def getMetaPath(path: String) = {
    s"${path.stripSuffix("/")}/meta"
  }
}
