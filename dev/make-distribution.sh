#!/bin/bash
mvn -DskipTests clean package  \
    -Ponline -Pscala-2.11  \
    -Phive-thrift-server \
    -Pspark-2.3.0 \
    -Pdsl \
    -Pcrawler \
    -Passembly \
    -Popencv-support \
    -Pcarbondata \
    -Pstreamingpro-spark-2.3.0-adaptor
