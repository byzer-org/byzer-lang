package org.apache.spark.sql.mlsql.session

import java.io.IOException

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{MLSQLSparkConst, SparkConf}

import scala.collection.mutable.{HashSet => MHSet}
import streaming.log.Logging


/**
  * Created by allwefantasy on 1/6/2018.
  */
class MLSQLSession(username: String,
                   password: String,
                   ipAddress: String,
                   withImpersonation: Boolean,
                   sessionManager: SessionManager,
                   opManager: MLSQLOperationManager,
                   sessionConf: Map[String, String] = Map()
                  ) extends Logging {


  @volatile private[this] var lastAccessTime: Long = System.currentTimeMillis()
  private[this] var lastIdleTime = 0L

  private[this] val activeOperationSet = new MHSet[String]()


  private[this] lazy val _mlsqlSparkSession = new MLSQLSparkSession(username, sessionConf)

  private[this] def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private[this] def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (activeOperationSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  def sparkSession: SparkSession = this._mlsqlSparkSession.sparkSession

  def mlsqlSparkSession: MLSQLSparkSession = this._mlsqlSparkSession

  def open(sessionConf: Map[String, String]): Unit = {
    mlsqlSparkSession.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      activeOperationSet.clear()
    } finally {
      release(true)
    }
  }


  def visit(): MLSQLSession = {
    acquire(true)
    release(true)
    this
  }


  def getUserName = username

  def getOpManager = opManager

}
