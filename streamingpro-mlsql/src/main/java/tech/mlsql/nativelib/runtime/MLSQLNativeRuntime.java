package tech.mlsql.nativelib.runtime;

/**
 * 19/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLNativeRuntime {
    static {
        JniUtils.loadLibrary(JniUtils.MLSQL_NATIVE_LIB, true,"/Volumes/Samsung_T5/projects/MlsqlNativeLib/build");
    }

    public static native String funcLower(String str);

    public static native String funcUpper(String str);
}

