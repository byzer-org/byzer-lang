load libsvm.`sample_libsvm_data.txt` as data;

train data as PythonAlg.`/tmp/pa_model`
where
pythonScriptPath="${pythonScriptPath}"
and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and `kafkaParam.userName`="zhuhl"
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"
and validateTable="data"
and `systemParam.pythonPath`="python"
and `systemParam.pythonParam`="-u"
and `systemParam.pythonVer`="2.7"
;

register PythonAlg.`/tmp/pa_model` as jack options
pythonScriptPath="${pythonPredictScriptPath}"
and algIndex="0"
and enableCopyTrainParamsToPython="true"
;

select jack(features) from data
as newdata;