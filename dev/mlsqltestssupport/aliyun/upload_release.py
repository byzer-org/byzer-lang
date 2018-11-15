# -*- coding: utf-8 -*-
import oss2
import os

if not os.environ['AK']:
    raise ValueError('AK and AKS should be configured')

if not os.environ['MLSQL_RELEASE_TAR']:
    raise ValueError('MLSQL_RELEASE_TAR should be configured')

fileName = os.environ['MLSQL_RELEASE_TAR']

auth = oss2.Auth(os.environ['AK'], os.environ['AKS'])
# Endpoint以杭州为例，其它Region请按实际情况填写。
bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'mlsql-release-repo')

bucket.create_bucket(oss2.models.BUCKET_ACL_PRIVATE)

bucket.put_object_from_file(fileName.split("/")[-1], fileName)

print("success uploaded")
