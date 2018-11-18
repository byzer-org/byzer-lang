# -*- coding: utf-8 -*-
import os
import mlsqltestssupport.aliyun.config as config


if not os.environ['MLSQL_RELEASE_TAR']:
    raise ValueError('MLSQL_RELEASE_TAR should be configured')

fileName = os.environ['MLSQL_RELEASE_TAR']

bucket = config.ossClient()

bucket.put_object_from_file(fileName.split("/")[-1], fileName)

print("success uploaded")
