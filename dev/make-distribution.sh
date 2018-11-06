#!/bin/bash
rm -rf /tmp/temp_ServiceFramework
git clone --depth 1 https://github.com/allwefantasy/ServiceFramework.git /tmp/temp_ServiceFramework
cd /tmp/temp_ServiceFramework
mvn install -DskipTests -Pjetty-9 -Pweb-include-jetty-9

cd -

mvn -DskipTests clean package -pl streamingpro-mlsql -am \
-Pspark-2.3.0 \
-Pstreamingpro-spark-2.3.0-adaptor \
-Ponline \
-Pscala-2.11 \
-Pdsl \
-Passembly \
-Pcrawler \
-Phive-thrift-server \
-Pautoml \
-Pxgboost \
-Pcarbondata \
-Popencv-support
