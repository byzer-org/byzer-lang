### TfIdfInPlace

TF/IDF is really widespread used in NLP classification job.
TfIdfInPlace can be used to convert raw text to vector with tf/idf.

A list of numeric is represented by Vector in StreamingPro  which is can be feed to many algorithms directly, and the element in vector
normally  is double type.

The processing steps include:

1. text analysis
2. filter with stopwords
3. ngram
4. word indexed with integer
5. compute idf/tf
6. weighted specified words

You can control how this module work by tuning the parameters TfIdfInPlace exposes.

Example:

```sql
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
-- optinal, a token specifying how to analysis the text string.
-- and split=" "
-- when analysis text content, we will not
-- consider POS if set true
and ignoreNature="true"
-- user-defined dictionary
and dicPaths="...."
-- user-defined stopword dictionary
and stopWordPath="/tmp/tfidf/stopwords"
-- words should be weighted
and priorityDicPath="/tmp/tfidf/prioritywords"
-- how much weight should be applied in priority words
and priority="5.0"
-- ngram，we can compose 2 or 3 words together so maby the new
-- complex features can more succinctly capture importtant information in
-- raw data. Note that too much ngram composition may increase feature space
-- too much , this makes it hard to compute.
and nGrams="2,3"
;

-- load the converting result.
load parquet.`/tmp/tfidf/data` as lwys_corpus_with_featurize;
-- register the model as predict function so we can use it in batch/stream/api applications
register TfIdfInPlace.`/tmp/tfidfinplace` as predict;
```


Parameters:

|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|Which text column you want to process||
|resultFeature|None|flag:concat m n-dim arrays to one m*n-dim array;merge: merge multi n-dim arrays into one n-dim array；index: output of conword sequence|
|dicPaths|None|user-defined dictionary|
|stopWordPath|user-defined stop word dictionary||
|priority||how much weight should be applied in priority words|
|nGrams|None|ngram，we can compose 2 or 3 words together so maby the new complex features can more succinctly capture importtant information in raw data. Note that too much ngram composition may increase feature space too much , this makes it hard to compute.|
|split|optinal, a token specifying how to analysis the text string||