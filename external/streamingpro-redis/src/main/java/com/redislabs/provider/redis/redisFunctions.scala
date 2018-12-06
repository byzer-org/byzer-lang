//package com.redislabs.provider.redis
//
//import com.redislabs.provider.redis.streaming.RedisInputDStream
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
//import com.redislabs.provider.redis.rdd._
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.StreamingContext
//
///**
//  * RedisContext extends sparkContext's functionality with redis functions
//  *
//  * @param sc a spark context
//  */
//class RedisContext(@transient val sc: SparkContext) extends Serializable {
//
//  import com.redislabs.provider.redis.RedisContext._
//
//  /**
//    * @param keyPattern a key pattern to match, or a single key
//    * @param partitionNum number of partitions
//    * @return RedisKeysRDD of simple Keys stored in redis server
//    */
//  def fromRedisKeyPattern(keyPattern: String = "*",
//                          partitionNum: Int = 3)
//                         (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RedisKeysRDD = {
//    new RedisKeysRDD(sc, redisConfig, keyPattern, partitionNum, null)
//  }
//
//  /**
//    * @param keys an array of keys
//    * @param partitionNum number of partitions
//    * @return RedisKeysRDD of simple Keys stored in redis server
//    */
//  def fromRedisKeys(keys: Array[String],
//                    partitionNum: Int = 3)
//                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RedisKeysRDD = {
//    new RedisKeysRDD(sc, redisConfig, "", partitionNum, keys)
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisKVRDD of simple Key-Values stored in redis server
//    */
//  def fromRedisKV[T](keysOrKeyPattern: T,
//                     partitionNum: Int = 3)
//                    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[(String, String)] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getKV
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getKV
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisListRDD of related values stored in redis server
//    */
//  def fromRedisList[T](keysOrKeyPattern: T,
//                       partitionNum: Int = 3)
//                      (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[String] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getList
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getList
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
//    */
//  def fromRedisSet[T](keysOrKeyPattern: T,
//                      partitionNum: Int = 3)
//                     (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[String] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getSet
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getSet
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisHashRDD of related Key-Values stored in redis server
//    */
//  def fromRedisHash[T](keysOrKeyPattern: T,
//                       partitionNum: Int = 3)
//                      (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[(String, String)] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getHash
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getHash
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
//    */
//  def fromRedisZSet[T](keysOrKeyPattern: T,
//                       partitionNum: Int = 3)
//                      (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[String] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSet
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSet
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of related Key-Scores stored in redis server
//    */
//  def fromRedisZSetWithScore[T](keysOrKeyPattern: T,
//                                partitionNum: Int = 3)
//                               (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[(String, Double)] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetWithScore
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetWithScore
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param start start position of target zsets
//    * @param end end position of target zsets
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
//    */
//  def fromRedisZRange[T](keysOrKeyPattern: T,
//                         start: Int,
//                         end: Int,
//                         partitionNum: Int = 3)
//                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[String] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByRange(start, end)
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByRange(start, end)
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param start start position of target zsets
//    * @param end end position of target zsets
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of related Key-Scores stored in redis server
//    */
//  def fromRedisZRangeWithScore[T](keysOrKeyPattern: T,
//                                  start: Int,
//                                  end: Int,
//                                  partitionNum: Int = 3)
//                                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[(String, Double)] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByRangeWithScore(start, end)
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByRangeWithScore(start, end)
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param min min score of target zsets
//    * @param max max score of target zsets
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of Keys in related ZSets stored in redis server
//    */
//  def fromRedisZRangeByScore[T](keysOrKeyPattern: T,
//                                min: Double,
//                                max: Double,
//                                partitionNum: Int = 3)
//                               (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[String] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByScore(min, max)
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByScore(min, max)
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param keysOrKeyPattern an array of keys or a key pattern
//    * @param min min score of target zsets
//    * @param max max score of target zsets
//    * @param partitionNum number of partitions
//    * @return RedisZSetRDD of related Key-Scores stored in redis server
//    */
//  def fromRedisZRangeByScoreWithScore[T](keysOrKeyPattern: T,
//                                         min: Double,
//                                         max: Double,
//                                         partitionNum: Int = 3)
//                                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
//  RDD[(String, Double)] = {
//    keysOrKeyPattern match {
//      case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByScoreWithScore(min, max)
//      case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByScoreWithScore(min, max)
//      case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
//    }
//  }
//
//  /**
//    * @param kvs Pair RDD of K/V
//    * @param ttl time to live
//    */
//  def toRedisKV(kvs: RDD[(String, String)], ttl: Int = 0)
//               (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    kvs.foreachPartition(partition => setKVs(partition, ttl, redisConfig))
//  }
//
//  /**
//    * @param kvs      Pair RDD of K/V
//    * @param hashName target hash's name which hold all the kvs
//    * @param ttl time to live
//    */
//  def toRedisHASH(kvs: RDD[(String, String)], hashName: String, ttl: Int = 0)
//                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    kvs.foreachPartition(partition => setHash(hashName, partition, ttl, redisConfig))
//  }
//
//  /**
//    * @param kvs      Pair RDD of K/V
//    * @param zsetName target zset's name which hold all the kvs
//    * @param ttl time to live
//    */
//  def toRedisZSET(kvs: RDD[(String, String)], zsetName: String, ttl: Int = 0)
//                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    kvs.foreachPartition(partition => setZset(zsetName, partition, ttl, redisConfig))
//  }
//
//  /**
//    * @param vs      RDD of values
//    * @param setName target set's name which hold all the vs
//    * @param ttl time to live
//    */
//  def toRedisSET(vs: RDD[String], setName: String, ttl: Int = 0)
//                (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    vs.foreachPartition(partition => setSet(setName, partition, ttl, redisConfig))
//  }
//
//  /**
//    * @param vs       RDD of values
//    * @param listName target list's name which hold all the vs
//    * @param ttl time to live
//    */
//  def toRedisLIST(vs: RDD[String], listName: String, ttl: Int = 0)
//                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    vs.foreachPartition(partition => setList(listName, partition, ttl, redisConfig))
//  }
//
//  /**
//    * @param vs       RDD of values
//    * @param listName target list's name which hold all the vs
//    * @param listSize target list's size
//    *                 save all the vs to listName(list type) in redis-server
//    */
//  def toRedisFixedLIST(vs: RDD[String],
//                       listName: String,
//                       listSize: Int = 0)
//                      (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
//    vs.foreachPartition(partition => setFixedList(listName, listSize, partition, redisConfig))
//  }
//}
//
//
//
//object RedisContext extends Serializable {
//  /**
//    * @param arr k/vs which should be saved in the target host
//    *            save all the k/vs to the target host
//    * @param ttl time to live
//    */
//  def setKVs(arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig) {
//    arr.map(kv => (redisConfig.getHost(kv._1), kv)).toArray.groupBy(_._1).
//      mapValues(a => a.map(p => p._2)).foreach {
//      x => {
//        val conn = x._1.endpoint.connect()
//        val pipeline = conn.pipelined
//        if (ttl <= 0) {
//          x._2.foreach(x => pipeline.set(x._1, x._2))
//        }
//        else {
//          x._2.foreach(x => pipeline.setex(x._1, ttl, x._2))
//        }
//        pipeline.sync
//        conn.close
//      }
//    }
//  }
//
//
//  /**
//    * @param hashName
//    * @param arr k/vs which should be saved in the target host
//    *            save all the k/vs to hashName(list type) to the target host
//    * @param ttl time to live
//    */
//  def setHash(hashName: String, arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig) {
//    val conn = redisConfig.connectionForKey(hashName)
//    val pipeline = conn.pipelined
//    arr.foreach(x => pipeline.hset(hashName, x._1, x._2))
//    if (ttl > 0) pipeline.expire(hashName, ttl)
//    pipeline.sync
//    conn.close
//  }
//
//  /**
//    * @param zsetName
//    * @param arr k/vs which should be saved in the target host
//    *            save all the k/vs to zsetName(zset type) to the target host
//    * @param ttl time to live
//    */
//  def setZset(zsetName: String, arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig) {
//    val conn = redisConfig.connectionForKey(zsetName)
//    val pipeline = conn.pipelined
//    arr.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
//    if (ttl > 0) pipeline.expire(zsetName, ttl)
//    pipeline.sync
//    conn.close
//  }
//
//  /**
//    * @param setName
//    * @param arr values which should be saved in the target host
//    *            save all the values to setName(set type) to the target host
//    * @param ttl time to live
//    */
//  def setSet(setName: String, arr: Iterator[String], ttl: Int, redisConfig: RedisConfig) {
//    val conn = redisConfig.connectionForKey(setName)
//    val pipeline = conn.pipelined
//    arr.foreach(pipeline.sadd(setName, _))
//    if (ttl > 0) pipeline.expire(setName, ttl)
//    pipeline.sync
//    conn.close
//  }
//
//  /**
//    * @param listName
//    * @param arr values which should be saved in the target host
//    *            save all the values to listName(list type) to the target host
//    * @param ttl time to live
//    */
//  def setList(listName: String, arr: Iterator[String], ttl: Int, redisConfig: RedisConfig) {
//    val conn = redisConfig.connectionForKey(listName)
//    val pipeline = conn.pipelined
//    arr.foreach(pipeline.rpush(listName, _))
//    if (ttl > 0) pipeline.expire(listName, ttl)
//    pipeline.sync
//    conn.close
//  }
//
//  /**
//    * @param key
//    * @param listSize
//    * @param arr values which should be saved in the target host
//    *            save all the values to listName(list type) to the target host
//    */
//  def setFixedList(key: String, listSize: Int, arr: Iterator[String], redisConfig: RedisConfig) {
//    val conn = redisConfig.connectionForKey(key)
//    val pipeline = conn.pipelined
//    arr.foreach(pipeline.lpush(key, _))
//    if (listSize > 0) {
//      pipeline.ltrim(key, 0, listSize - 1)
//    }
//    pipeline.sync
//    conn.close
//  }
//}
//
///**
//  * RedisStreamingContext extends StreamingContext's functionality with Redis
//  *
//  * @param ssc a spark StreamingContext
//  */
//class RedisStreamingContext(@transient val ssc: StreamingContext) extends Serializable {
//  /**
//    * @param keys an Array[String] which consists all the Lists we want to listen to
//    * @param storageLevel the receiver' storage tragedy of received data, default as MEMORY_AND_DISK_2
//    * @return a stream of (listname, value)
//    */
//  def createRedisStream(keys: Array[String],
//                        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
//                       (implicit redisConfig: RedisConfig = new RedisConfig(new
//                           RedisEndpoint(ssc.sparkContext.getConf))) = {
//      new RedisInputDStream(ssc, keys, storageLevel, redisConfig, classOf[(String, String)])
//  }
//  /**
//    * @param keys an Array[String] which consists all the Lists we want to listen to
//    * @param storageLevel the receiver' storage tragedy of received data, default as MEMORY_AND_DISK_2
//    * @return a stream of (value)
//    */
//  def createRedisStreamWithoutListname(keys: Array[String],
//                        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2)
//                       (implicit redisConfig: RedisConfig = new RedisConfig(new
//                           RedisEndpoint(ssc.sparkContext.getConf))) = {
//      new RedisInputDStream(ssc, keys, storageLevel, redisConfig, classOf[String])
//  }
//}
//
//trait RedisFunctions {
//  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
//  implicit def toRedisStreamingContext(ssc: StreamingContext): RedisStreamingContext = new RedisStreamingContext(ssc)
//}
//
