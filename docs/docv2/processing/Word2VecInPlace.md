### Word2VecInPlace

Word2VecInPlace is similar with TfIdfInPlace. The difference is Word2VecInPlace works on word embedding tech.

The processing steps include:

1. text analysis
2. filter with stopwords
3. ngram
4. word indexed with integer
5. convert integer to vector
6. returns 1/2d array according to the requirements

If you declare word-embedding directory explicitly, Word2VecInPlace will skip the step training with word2vec algorithm.

Example：

```sql
load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
and ignoreNature="true"
and stopWordPath="/tmp/tfidf/stopwords"
and resultFeature="flat";

load parquet.`/tmp/word2vecinplace/data`
as lwys_corpus_with_featurize;

register Word2VecInPlace.`/tmp/tfidfinplace` as predict;
```

Parameters:

|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|None||
|resultFeature|None|flag:concat m n-dim arrays to one m*n-dim array;merge: merge multi n-dim arrays into one n-dim array；index: output of conword sequence|
|dicPaths|None|user-defined dictionary|
|wordvecPaths|None|you can specify the location of existed word2vec model|
|vectorSize|None|the  word vector size you expect|
|length|None|input sentence length|
|stopWordPath|user-defined stop word dictionary||
|split|optinal, a token specifying how to analysis the text string||
|minCount|None||