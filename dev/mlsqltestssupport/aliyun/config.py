# -*- coding: utf-8 -*-

import os
import oss2

oss_endpoint = "http://oss-cn-hangzhou.aliyuncs.com"
bucket_name = "mlsql-release-repo"


def ossClient():
    if not os.environ['AK']:
        raise ValueError('AK and AKS should be configured')

    auth = oss2.Auth(os.environ['AK'], os.environ['AKS'])

    return oss2.Bucket(auth, oss_endpoint, bucket_name)
