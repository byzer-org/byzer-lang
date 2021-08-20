#!/usr/bin/env bash
## example
## ./dev/run-test.sh

mvn clean install -DskipTests
mvn test -pl streamingpro-it