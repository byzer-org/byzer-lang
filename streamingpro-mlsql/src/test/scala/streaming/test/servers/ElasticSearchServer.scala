package streaming.test.servers

/**
  * 2018-12-19 WilliamZhu(allwefantasy@gmail.com)
  */
class ElasticSearchServer(version: String) extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2'
       |services:
       |  elasticsearch:
       |    image: library/elasticsearch:${version}
       |    ports:
       |      - "9200:9200"
       |      - "9300:9300"
       |    environment:
       |      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
       |      discovery.type: single-node
    """.stripMargin

  override def waitToServiceReady: Boolean = {
    // wait zk to ready, runs on host server
    val shellCommand = s"curl -XGET http://127.0.0.1:9200/_cat/indices?v"
    readyCheck("", shellCommand, false)
  }
}
