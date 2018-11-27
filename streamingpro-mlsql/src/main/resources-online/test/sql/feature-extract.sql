train FeatureExtractInPlaceData as FeatureExtractInPlace.`/tmp/featureExtractInPlace`
where `inputCol`="doc"
;

register FeatureExtractInPlace.`/tmp/featureExtractInPlace` as mpredict;