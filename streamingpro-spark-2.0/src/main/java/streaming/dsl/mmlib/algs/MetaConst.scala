package streaming.dsl.mmlib.algs

/**
  * Created by allwefantasy on 22/5/2018.
  */
object MetaConst {
  def TF_IDF_PATH(path: String, col: String) = s"$path/columns/$col/tfidf"

  def WORD2VEC_PATH(path: String, col: String) = s"$path/columns/$col/word2vec"

  def WORD_INDEX_PATH(path: String, col: String) = s"$path/columns/$col/wordIndex"

  def PARAMS_PATH(path: String, col: String) = s"$path/params"

  def getDataPath(path: String) = {
    s"${path.stripSuffix("/")}/data"
  }

  def getMetaPath(path: String) = {
    s"${path.stripSuffix("/")}/meta"
  }
}
