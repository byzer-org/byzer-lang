package tech.mlsql.indexer

case class MlsqlOriTable(name:String,format:String,path:String,storageName:String,options:Map[String,String])
case class MlsqlIndexerItem(name: String,
                            oriFormat: String,
                            oriPath: String,
                            oriStorageName: String,
                            format: String,
                            path: String,
                            storageName: String,
                            status: Int,
                            owner:String,
                            lastStatus: Int,
                            lastFailMsg: String,
                            lastExecuteTime: Long,
                            syncInterval: Long,
                            content: String,
                            indexerConfig: String,
                            lastJobId: String,
                            indexerType: String
                       ) {
  def isRealTime = syncInterval == -1

  def isOneTime = syncInterval == 0

  def isRepeat = syncInterval > 0
}

object MlsqlIndexerItem {
  val STATUS_NONE = 0
  val STATUS_INDEXING = 1

  val LAST_STATUS_SUCCESS = 0
  val LAST_STATUS_FAIL = 1

  val REAL_TIME = -1
  val ONE_TIME = 0

  val INDEXER_TYPE_MYSQL = "mysql"
  val INDEXER_TYPE_OTHER = "other"
  val INDEXER_TYPE_CUBE = "cube"
  val INDEXER_TYPE_MV = "mv"
}