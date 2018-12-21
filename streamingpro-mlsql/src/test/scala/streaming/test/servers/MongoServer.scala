package streaming.test.servers

/**
  * 2018-12-21 WilliamZhu(allwefantasy@gmail.com)
  */
class MongoServer(version: String) extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2'
       |
       |networks:
       |  app-tier:
       |    driver: bridge
       |
       |services:
       |  mongodb:
       |    image: 'bitnami/mongodb:4.0'
       |    ports:
       |      - "27017:27017"
    """.stripMargin

  override def waitToServiceReady: Boolean = {
    // wait mongo to ready, runs on host server
    val shellCommand = s"nc -z 127.0.0.1 27017"
    readyCheck("", shellCommand, false)
  }
}
