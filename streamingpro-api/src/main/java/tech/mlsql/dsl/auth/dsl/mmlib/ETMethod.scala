package tech.mlsql.dsl.auth.dsl.mmlib

/**
  * Created by aston on 2019/5/15.
  */

object ETMethod extends Enumeration {
  type ETMethod = Value
  val TRAIN = Value("train")
  val RUN = Value("run")
  val LOAD = Value("load")
  val PREDICT = Value("predict")
  val BATCH_PREDICT = Value("batchPredict")
}