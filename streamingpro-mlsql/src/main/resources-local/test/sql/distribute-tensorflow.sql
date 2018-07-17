load libsvm.`sample_libsvm_data.txt` as data;

train data as DTFAlg.`${path}`
where
pythonScriptPath="${pythonScriptPath}"
and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and  keepVersion="true"
and  enableDataLocal="true"
and  dataLocalFormat="json"
and distributeEveryExecutor="${distributeEveryExecutor}"

and  `fitParam.0.jobName`="worker"
and  `fitParam.0.taskIndex`="0"

and  `fitParam.1.jobName`="worker"
and  `fitParam.1.taskIndex`="1"

and  `fitParam.2.jobName`="ps"
and  `fitParam.2.taskIndex`="0"


and `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
;