# -*- coding: utf-8 -*-
import os
import oss2
import mlsqltestssupport.aliyun.config as config

if not os.environ['MLSQL_RELEASE_TAR']:
    raise ValueError('MLSQL_RELEASE_TAR should be configured')

fileName = os.environ['MLSQL_RELEASE_TAR']

if not os.environ['AK']:
    raise ValueError('AK and AKS should be configured')

auth = oss2.Auth(os.environ['AK'], os.environ['AKS'])

bucket = oss2.Bucket(auth, "oss-cn-hangzhou-internal.aliyuncs.com", config.bucket_name)

bucket.get_object_to_file(fileName.split("/")[-1], fileName)

print("download successful")
