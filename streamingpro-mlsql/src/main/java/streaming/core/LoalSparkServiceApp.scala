/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.core

/**
  * Created by allwefantasy on 30/3/2017.
  */
object LocalSparkServiceApp {
  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "god",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      //"-spark.mlsql.enable.max.result.limit", "true",
      //"-spark.mlsql.restful.api.max.result.size", "7",
      //      "-spark.mlsql.enable.datasource.rewrite", "true",
      //      "-spark.mlsql.datasource.rewrite.implClass", "streaming.core.datasource.impl.TestRewrite",
      //"-streaming.job.file.path", "classpath:///test/empty.json",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.ps.enable", "true",
      "-spark.sql.hive.thriftServer.singleSession", "true",
      "-streaming.rest.intercept.clzz", "streaming.rest.ExampleRestInterceptor",
      "-streaming.deploy.rest.api", "true",
      "-spark.driver.maxResultSize", "2g",
      "-spark.serializer", "org.apache.spark.serializer.KryoSerializer",
      "-spark.sql.codegen.wholeStage", "true",
      "-spark.kryoserializer.buffer.max", "2000m",
//      "-spark.mlsql.enable.runtime.select.auth", "true",
      "-streaming.driver.port", "9003",
      "-spark.files.maxPartitionBytes", "10485760",
      "-spark.sql.shuffle.partitions", "1",
      "-spark.hadoop.mapreduce.job.run-local", "true"

      //"-streaming.sql.out.path","file:///tmp/test/pdate=20160809"

      //"-streaming.jobs","idf-compute"
      //"-streaming.sql.source.path","hdfs://m2:8020/data/raw/live-hls-formated/20160725/19/cdn148-16-52_2016072519.1469444764341"
      //"-streaming.driver.port", "9005"
      //"-streaming.zk.servers", "127.0.0.1",
      //"-streaming.zk.conf_root_dir", "/streamingpro/jack"
    ))
  }
}
