CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(mlsql_runtime_native_lib)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

FIND_PACKAGE(JNI REQUIRED)

IF (${CMAKE_HOST_SYSTEM_NAME} MATCHES "Linux")
    set(SRC_COMMON "./src/common/linuxsys.h" "./src/common/linuxsys.cpp")
ELSE ()
    set(SRC_COMMON "./src/common/macossys.h" "./src/common/macossys.cpp")
ENDIF ()

INCLUDE_DIRECTORIES(SYSTEM ${JNI_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(SYSTEM src)

ADD_LIBRARY(mlsql_runtime_native_lib SHARED ${SRC_COMMON} src/native_lib_for_java/tech_mlsql_nativelib_MLSQLNativeRuntime.cc)
ADD_EXECUTABLE(main ${SRC_COMMON} src/main.cpp)