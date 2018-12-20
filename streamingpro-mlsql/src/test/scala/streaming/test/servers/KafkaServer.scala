package streaming.test.servers

/**
  * 2018-12-19 WilliamZhu(allwefantasy@gmail.com)
  */
class KafkaServer(version: String) extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2'
       |services:
       |  zookeeper:
       |    image: wurstmeister/zookeeper
       |    ports:
       |      - "2181:2181"
       |  kafka:
       |    image: wurstmeister/kafka:2.11-${version}
       |    depends_on:
       |      - zookeeper
       |    ports:
       |      - "9092:9092"
       |    environment:
       |      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://127.0.0.1:9092
       |      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
       |      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    """.stripMargin

  override def waitToServiceReady: Boolean = {
    // wait zk to ready, runs on host server
    var shellCommand = s"echo stat | nc 127.0.0.1 2181"
    readyCheck("", shellCommand, false)
    // wait kafka to ready
    // because we have no kafka-topics.sh in server ,so we should execute the command
    // in kafka docker container . please notice that we use zookeeper instead of 127.0.0.1 as the zk address.
    shellCommand = s"kafka-topics.sh --list  --zookeeper zookeeper:2181"
    readyCheck("kafka", shellCommand, true)
  }
}
