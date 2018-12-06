package org.apache.spark.sql.mlsql.session

import streaming.log.Logging


/**
  * Created by allwefantasy on 4/6/2018.
  */
class MLSQLOperation(session: MLSQLSession, f: () => Unit, opId: String, desc: String, operationTimeout: Int) extends Logging {


}
