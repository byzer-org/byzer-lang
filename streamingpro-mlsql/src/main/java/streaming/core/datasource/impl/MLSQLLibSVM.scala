package streaming.core.datasource.impl

import streaming.core.datasource.MLSQLBaseFileSource
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-04-18 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLLibSVM(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "libsvm"

  override def shortFormat: String = "libsvm"

}
