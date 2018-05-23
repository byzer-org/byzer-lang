mvn -DskipTests clean package  -pl streamingpro-spark-2.0 -am  -Ponline -Pscala-2.11  -Phive-thrift-server -Pspark-2.2.0 -Pdsl-legacy  -Pshade -Pcarbondata -Pcrawler
