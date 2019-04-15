### LDA

```sql
load libsvm.`D:/soucecode/spark-2.3-src/data/mllib/sample_lda_libsvm_data.txt` as data;
train data as LDA.`C:/tmp/model`;
register LDA.`C:/tmp/model` as lda;
select label,lda(4) topicsMatrix,lda_doc(features) TopicDistribution,lda_topic(label,4) describeTopics from data as result;
save overwrite result as json.`/tmp/result`;
```