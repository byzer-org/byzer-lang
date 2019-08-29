package tech.mlsql.test

/**
  * 2019-06-21 WilliamZhu(allwefantasy@gmail.com)
  */
object TestData {
  val streamData =
    """
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
      |{"key":"yes","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
    """.stripMargin

}

object StreamSubBatchQuerySuiteData {
  def streamCode(code: String) =
    s"""
       |-- the stream name, should be uniq.
       |set streamName="streamExample";
       |
      |
      |-- mock some data.
       |set data='''
       |${TestData.streamData}
       |''';
       |
      |-- load data as table
       |load jsonStr.`data` as datasource;
       |
      |-- convert table as stream source
       |load mockStream.`datasource` options
       |stepSizeRange="0-3"
       |as newkafkatable1;
       |
      |select cast(value as string) as k  from newkafkatable1
       |as table21;
       |
      |save append table21
       |as custom.``
       |options mode="append"
       |and duration="15"
       |and sourceTable="jack"
       |and code='''
       |${code}
       |'''
       |and checkpointLocation="/tmp/cpl15";
       |
    """.stripMargin
}

object JobManagerSuiteData {
  val streamCode =
    s"""
       |-- the stream name, should be uniq.
       |set streamName="streamExample";
       |
      |
      |-- mock some data.
       |set data='''
       |${TestData.streamData}
       |''';
       |
      |-- load data as table
       |load jsonStr.`data` as datasource;
       |
      |-- convert table as stream source
       |load mockStream.`datasource` options
       |stepSizeRange="0-3"
       |as newkafkatable1;
       |
      |-- aggregation
       |select cast(key as string) as k,count(*) as c  from newkafkatable1 group by key
       |as table21;
       |
      |-- output the the result to console.
       |save append table21
       |as console.``
       |options mode="Complete"
       |and duration="15"
       |and checkpointLocation="/tmp/cpl3";
       |
    """.stripMargin
}
