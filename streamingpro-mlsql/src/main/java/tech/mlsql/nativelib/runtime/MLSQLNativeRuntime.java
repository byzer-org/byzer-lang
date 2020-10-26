package tech.mlsql.nativelib.runtime;

/**
 * 19/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLNativeRuntime {
    static {
        //"/Volumes/Samsung_T5/projects/MlsqlNativeLib/build"
        JniUtils.loadLibrary(JniUtils.MLSQL_NATIVE_LIB);
    }

    public static native String funcLower(String str);

    public static native String funcUpper(String str);
}

