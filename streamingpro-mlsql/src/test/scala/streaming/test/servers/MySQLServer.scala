package streaming.test.servers

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MySQLServer(version: String) extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2'
       |services:
       |  mysql:
       |    image: mysql:${version}
       |    command: --default-authentication-plugin=mysql_native_password
       |    restart: always
       |    ports:
       |      - "3306:3306"
       |    environment:
       |      MYSQL_ROOT_PASSWORD: mlsql
       |      discovery.type: single-node
    """.stripMargin

  override def waitToServiceReady: Boolean = {
    // wait zk to ready, runs on host server
    val shellCommand = s"exec mysql -uroot -pmlsql --protocol=tcp -e 'SHOW CHARACTER SET'"
    readyCheck("mysql", shellCommand, true)
  }
}