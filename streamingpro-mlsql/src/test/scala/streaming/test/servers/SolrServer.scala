package streaming.test.servers


/**
  * Created by pigeongeng on 2018/12/27.4:55 PM
  *
  */
class SolrServer(version: String= "latest") extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2'
       |services:
       |  solr:
       |    image: solr
       |    ports:
       |      - "9983:9983"
       |      - "8983:8983"
       |    entrypoint:
       |      - docker-entrypoint.sh
       |      - solr
       |      - start
       |      - -f
       |      - -h
       |      - 127.0.0.1
       |      - -noprompt
       |      - -cloud
    """.stripMargin

  /**
    * create a collection with the name mlsql_example
    * check it is successful
    * */
  override def waitToServiceReady: Boolean = {
    // wait mongo to ready, runs on host server
    val shellCommand = s"curl 'http://localhost:8983/solr/admin/collections?action=CREATE&name=mlsql_example&numShards=1&replicationFactor=2&maxShardsPerNode=2&collection.configName=_default'"
    readyCheck("", shellCommand, false)
  }
}
