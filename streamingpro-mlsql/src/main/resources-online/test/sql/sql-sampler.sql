load libsvm.`sample_libsvm_data.txt` as data;
train data as RateSampler.`/tmp/sample` where labelCol="label"
and sampleRate="0.8,0.1,0.1";
load parquet.`/tmp/sample`
as sample_data;