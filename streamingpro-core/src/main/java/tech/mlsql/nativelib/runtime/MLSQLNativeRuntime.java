package tech.mlsql.nativelib.runtime;

/**
 * 19/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLNativeRuntime {
    static {
        //"/Volumes/Samsung_T5/projects/MlsqlNativeLib/build"
        JniUtils.loadLibrary(JniUtils.MLSQL_NATIVE_LIB, true,
                "/Users/qingwang/workspace/opensource/mlsql/external/mlsql-native-operators/cmake-build-debug");
    }

    public static native String funcLower(String str);

    public static native String funcUpper(String str);

    public static native float getCPULoad();

}

