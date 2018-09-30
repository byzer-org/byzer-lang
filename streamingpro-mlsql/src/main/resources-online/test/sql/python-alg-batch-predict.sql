load libsvm.`sample_libsvm_data.txt` as data;

train data as PythonAlgBP.`${path}`
where
pythonScriptPath="${pythonScriptPath}"

and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"

and  `fitParam.modelPath`="${HOME}/${modelPath}"

and `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
;
