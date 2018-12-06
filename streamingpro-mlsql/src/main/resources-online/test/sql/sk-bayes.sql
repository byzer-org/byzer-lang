-- sklearn MultinomialNB
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

train data as SKLearn.`/tmp/model` 
where pythonDescPath=""
and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"
and  `fitParam.0.alg`="MultinomialNB"
and `systemParam.pythonPath`="python";
;
register SKLearn.`/tmp/model` as nb_predict;