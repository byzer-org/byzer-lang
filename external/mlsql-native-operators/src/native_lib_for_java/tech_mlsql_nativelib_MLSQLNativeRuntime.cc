#include "native_lib_for_java/tech_mlsql_nativelib_runtime_MLSQLNativeRuntime.h"

#include <iostream>
#include <string>
#include <algorithm>

/// Convert a Java String to C++ std::string.
inline std::string JavaStringToNativeString(JNIEnv *env, jstring &jstr) {
    const char *c_str = env->GetStringUTFChars(jstr, nullptr);
    std::string result(c_str);
    env->ReleaseStringUTFChars(static_cast<jstring>(jstr), c_str);
    return result;
}

/// Convert C++ String to a Java ByteArray.
inline jstring NativeStringToJavaString(JNIEnv *env, const std::string &str) {
    return env->NewStringUTF(reinterpret_cast<const char *>(str.c_str()));
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     tech_mlsql_nativelib_runtime_MLSQLNativeRuntime
 * Method:    funcLower
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_tech_mlsql_nativelib_runtime_MLSQLNativeRuntime_funcLower
        (JNIEnv *env, jclass cls, jstring jstr) {
    auto native_str = JavaStringToNativeString(env, jstr);

    // TODO(qwang): If the string contains non-ASCII character set, skip it.
    std::transform(native_str.begin(), native_str.end(), native_str.begin(),
                   [](unsigned char ch){ return std::tolower(ch); });
    return NativeStringToJavaString(env, native_str);
}

/*
 * Class:     tech_mlsql_nativelib_runtime_MLSQLNativeRuntime
 * Method:    funcUpper
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_tech_mlsql_nativelib_runtime_MLSQLNativeRuntime_funcUpper
        (JNIEnv *env, jclass cls, jstring jstr) {
    auto native_str = JavaStringToNativeString(env, jstr);

    std::transform(native_str.begin(), native_str.end(), native_str.begin(),
                   [](unsigned char ch){ return std::toupper(ch); });
    return NativeStringToJavaString(env, native_str);
}

JNIEXPORT jfloat JNICALL Java_tech_mlsql_nativelib_runtime_MLSQLNativeRuntime_getCPULoad
        (JNIEnv *env, jclass cls) {
    return GetCPULoad();
}


#ifdef __cplusplus
}
#endif
