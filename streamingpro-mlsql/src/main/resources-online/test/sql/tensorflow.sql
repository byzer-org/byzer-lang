-- tensorflow
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

select onehot(label,2) as label,features from data
as newdata;


select * from newdata limit 2
as data1;


train newdata as TensorFlow.`/tmp/model2`
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and   `kafkaParam.topic`="test"
and   `kafkaParam.group_id`="g_test-1"
and   `kafkaParam.reuse`="false"
and   `fitParam.0.layerGroup`="300,100"
and   `fitParam.0.epochs`="1"
and   `fitParam.0.batchSize`="32"
and   `fitParam.0.featureSize`="692"
and   `fitParam.0.labelSize`="2"
and   `fitParam.0.alg`="FCClassify"
and  `validateTable`="data1"
and   `systemParam.pythonPath`="python";

register  TensorFlow.`/tmp/model2`  as tf_predict;