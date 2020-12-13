##
PROJECT_HOME=/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/streamingpro-spark-2.4.x/external/mlsql-native-operators
cd ${PROJECT_HOME}
PREFIX=../../streamingpro-mlsql/src/main

for mode in local online
do
    cp -f cmake-build-debug-ubuntu/libmlsql_runtime_native_lib.so ${PREFIX}/resources-${mode}/native/linux
    cp -f build/libmlsql_runtime_native_lib.dylib ${PREFIX}/resources-${mode}/native/darwin
done
