package tech.mlsql.nativelib.runtime;

/**
 * 19/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class MLSQLNativeRuntime {
    static {
        JniUtils.loadLibrary(JniUtils.MLSQL_NATIVE_LIB);
    }

    public static native String funcLower(String str);

    public static native String funcUpper(String str);

    public static native float getCPULoad();

}

