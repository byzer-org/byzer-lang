PROJECT_HOME=/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/streamingpro-spark-2.4.x/external/mlsql-native-operators
cd ${PROJECT_HOME}
PREFIX=/Users/allwefantasy/CSDNWorkSpace/streamingpro-spark-2.4.x/streamingpro-mlsql/src/main

cp -f cmake-build-debug-ubuntu/libmlsql_runtime_native_lib.so ${PREFIX}/resources-local/native/linux
cp -f build/libmlsql_runtime_native_lib.dylib ${PREFIX}/resources-local/native/darwin

cp -f cmake-build-debug-ubuntu/libmlsql_runtime_native_lib.so ${PREFIX}/resources-online/native/linux
cp -f build/libmlsql_runtime_native_lib.dylib ${PREFIX}/resources-online/native/darwin