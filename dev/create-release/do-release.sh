#!/usr/bin/env bash

export GIT_NAME=allwenfatasy
export GIT_EMAIL=allwefantasy@gmail.com
export GIT_BRANCH=branch-1.2.0

export RELEASE_VERSION=1.2.0
export RELEASE_TAG=v1.2.0
export NEXT_VERSION=1.2.1
export DRY_RUN=1

# ./release-tag.sh

export GIT_REF=v1.1.4-test
export MLSQL_PACKAGE_VERSION=1.1.4-rc
export MLSQL_VERSION=1.1.4
export MLSQL_SPARK_VERSIOIN=2.3.0

./release-build.sh package