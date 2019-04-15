@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem Figure out where the Spark framework is installed

rem Test that an argument was given
echo %1|findstr "help" >nul

if %errorlevel% equ 0 (
  call:exit_with_usage
  exit /b 1
)

rem Figure out where spark_version is.
set SPARK_VERSIOIN=spark_version
if not "x%MLSQL_SPARK_VERSIOIN%"=="x" (
  set SPARK_VERSIOIN=%MLSQL_SPARK_VERSIOIN%
) else (
  where /q "%SPARK_VERSIOIN%"
  if ERRORLEVEL 1 (
    echo MLSQL_SPARK_VERSIOIN environment variable is not set.
    exit /b 1
  )
)

rem Figure out where dry_run is.
set DRY=spark_version
if not "x%DRY_RUN%"=="x" (
  set DRY=%DRY_RUN%
) else (
  where /q "%DRY%"
  if ERRORLEVEL 1 (
    echo DRY_RUN environment variable is not set.
    exit /b 1
  )
)

rem cd the bat file dictiory.
cd /d %~dp0

set MLSQL_PATH=%cd%

rd %temp%\temp_ServiceFramework /s /q
git clone --depth 1 https://github.com/allwefantasy/ServiceFramework.git  %temp%\temp_ServiceFramework
cd /d %temp%\temp_ServiceFramework
call mvn install -DskipTests -Pjetty-9 -Pweb-include-jetty-9
rd %temp%\temp_ServiceFramework /s /q

cd /d %MLSQL_PATH%

set BASE_PROFILES=-Pscala-2.11 -Ponline -Phive-thrift-server -Pcarbondata -Pcrawler

if %SPARK_VERSIOIN% GTR 2.2 (
  set BASE_PROFILES1=%BASE_PROFILES% -Pdsl -Pxgboost
) else (
  set BASE_PROFILES1=%BASE_PROFILES% -Pdsl-legacy
  echo %BASE_PROFILES1%
)

set MAVEN_OPTS=-Xmx6000m
set BASE_PROFILES2=%BASE_PROFILES1% -Pspark-%SPARK_VERSIOIN%.0 -Pstreamingpro-spark-%SPARK_VERSIOIN%.0-adaptor
set RUN_MVN=mvn clean package -Pshade -DskipTests -pl streamingpro-mlsql -am %BASE_PROFILES2%

 if %DRY% EQU TRUE (
   echo %RUN_MVN%
 ) else (
   cd ..
   echo %RUN_MVN%
   call %RUN_MVN%
 )

exit /b 1

:: echo.&pause&goto:eof
 
::--------------------------------------------------------
::-- Function section starts below here
::--------------------------------------------------------
:exit_with_usage    - here starts my function identified by it`s label
echo.usage: package
echo.run package command based on different spark version.
echo.Inputs are specified with the following environment variables:
echo.
echo.MLSQL_SPARK_VERSIOIN -the spark version, 2.2/2.3/2.4
echo.DRY_RUN "true|false"
goto:eof