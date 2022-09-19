package tech.mlsql.it.utils

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.{InspectContainerResponse, InspectExecResponse}
import com.github.dockerjava.api.exception.NotFoundException
import com.github.dockerjava.api.model.{Frame, StreamType}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkCoreVersion
import org.slf4j.{Logger, LoggerFactory}
import tech.mlsql.core.version.MLSQLVersion
import tech.mlsql.it.docker.beans.{ContainerExecException, ContainerExecResult, ContainerExecResultBytes}
import tech.mlsql.tool.OrderedProperties
import tech.mlsql.tool.OrderedProperties.loadPropertiesFromInputStream

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardCopyOption}
import java.text.MessageFormat
import java.util.Optional
import java.util.concurrent.{CompletableFuture, ExecutionException}
import java.util.zip.GZIPOutputStream


object DockerUtils {
  private val LOG: Logger = LoggerFactory.getLogger(DockerUtils.getClass)

  def getByzerVersion: String = {
    var mlsqlVersion: String = MLSQLVersion.version().version
    if (mlsqlVersion.equals("${pom.version}")) {
      val testClassPath: String = DockerUtils.getClass.getResource(File.separator).getPath
      val mavenArch: File = new File(testClassPath + ".." + File.separator + "maven-archiver")
      val pomFile = new File(mavenArch.getCanonicalPath, "pom.properties")
      if (pomFile == null || !pomFile.exists) {
        throw new RuntimeException("fail to locate pom.properties")
      }
      val trimProps = OrderedProperties.copyAndTrim(loadPropertiesFromInputStream(new FileInputStream(pomFile)))
      mlsqlVersion = trimProps.getProperty("version")
    }
    mlsqlVersion
  }

  /**
   * e.g. byzer-lang-3.1.1-2.12-2.3.0-SNAPSHOT.jar
   *
   * @return
   */
  def getJarName: String = {
    var base: String = System.getProperty("maven.finalJarName")
    if (base == null) {
      val byzerVersion = getByzerVersion
      var scalaVersion: String = scala.tools.nsc.Properties.versionNumberString
      scalaVersion = if (StringUtils.isNoneBlank(scalaVersion) && scalaVersion.length >= 4) {
        scalaVersion.substring(0, 4)
      } else {
        "2.12"
      }
      base = String.format("byzer-lang-%s-%s-%s.jar", getSparkVersion, scalaVersion, byzerVersion)
    }
    base
  }

  def getSparkVersion: String = {
    SparkCoreVersion.exactVersion
  }

  def getLibPath: String = {
    MessageFormat.format("{0}{1}" + File.separator + "main" + File.separator, getRootPath, getFinalName)
  }

  /**
   * Get the byzer project absolute path. e.g.: /opt/project/byzer-lang/
   *
   * @return
   */
  def getRootPath: String = {
    var base: String = null
    try {
      val testClassPath: String = DockerUtils.getClass.getResource(File.separator).getPath
      val directory: File = new File(testClassPath + ".." + File.separator + ".." + File.separator + ".." + File.separator)
      base = directory.getCanonicalPath + File.separator
    } catch {
      case e: IOException =>
        LOG.error("Can not get lib path, try to use absolute path. e:" + e.getMessage)
    }
    if (base == null) {
      base = "." + File.separator
    }
    base
  }

  /**
   * Get the byzer streamingpro-it absolute path. e.g.: /opt/project/byzer-lang/streamingpro-it/
   *
   * @return
   */
  def getCurProjectRootPath: String = {
    var base: String = null
    try {
      val testClassPath: String = DockerUtils.getClass.getResource(File.separator).getPath
      val directory: File = new File(testClassPath + ".." + File.separator + ".." + File.separator)
      base = directory.getCanonicalPath + File.separator
    } catch {
      case e: IOException =>
        LOG.error("Can not get lib path, try to use absolute path. e:" + e.getMessage)
    }
    if (base == null) {
      base = "." + File.separator
    }
    base
  }

  def getFinalName: String = {
    var base: String = System.getProperty("maven.finalName")
    if (base == null) {
      val byzerVersion = getByzerVersion
      val sparkVersion: String = getSparkVersion
      base = String.format("byzer-lang-%s-%s", sparkVersion, byzerVersion)
    }
    base
  }

  def dumpContainerLogToTarget(dockerClient: DockerClient, containerId: String): Unit = {
    val containerName: String = getContainerName(dockerClient, containerId)
    val output: File = getUniqueFileInTargetDirectory(containerName, "docker", ".log")
    try {
      val os: OutputStream = new BufferedOutputStream(new FileOutputStream(output))
      try {
        val future: CompletableFuture[Boolean] = new CompletableFuture[Boolean]
        dockerClient.logContainerCmd(containerName).withStdOut(true).withStdErr(true)
          .withTimestamps(true).exec(new ResultCallback[Frame]() {
          override def close(): Unit = {
          }

          override def onStart(closeable: Closeable): Unit = {
          }

          override def onNext(`object`: Frame): Unit = {
            try os.write(`object`.getPayload)
            catch {
              case e: IOException =>
                onError(e)
            }
          }

          override def onError(throwable: Throwable): Unit = {
            future.completeExceptionally(throwable)
          }

          override def onComplete(): Unit = {
            future.complete(true)
          }
        })
        future.get
      } catch {
        case e@(_: RuntimeException | _: ExecutionException | _: IOException) =>
          LOG.error("Error dumping log for " + containerName, e)
        case ie: InterruptedException =>
          Thread.currentThread.interrupt()
          LOG.info("Interrupted dumping log from container " + containerName, ie)
      } finally {
        if (os != null) os.close()
      }
    }
  }

  def dumpContainerDirToTargetCompressed(dockerClient: DockerClient, containerId: String, path: String): Unit = {
    val containerName: String = getContainerName(dockerClient, containerId)
    val baseName: String = path.replace(File.separator, "-").replaceAll("^-", "")
    val output: File = getUniqueFileInTargetDirectory(containerName, baseName, ".tar.gz")
    try {
      val dockerStream: InputStream = dockerClient.copyArchiveFromContainerCmd(containerId, path).exec
      val os: OutputStream = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(output)))
      try IOUtils.copy(dockerStream, os)
      catch {
        case e@(_: RuntimeException | _: IOException) =>
          if (!e.isInstanceOf[NotFoundException]) {
            LOG.error("Error reading dir from container " + containerName, e)
          }
      } finally {
        if (dockerStream != null) dockerStream.close()
        if (os != null) os.close()
      }
    }
  }

  private def getUniqueFileInTargetDirectory(containerName: String, prefix: String, suffix: String): File = {
    return getUniqueFileInDirectory(getTargetDirectory(containerName), prefix, suffix)
  }

  private def getTargetDirectory(containerId: String): File = {
    var base: String = System.getProperty("maven.buildDirectory")
    if (base == null) {
      try {
        val testClassPath: String = DockerUtils.getClass.getResource(File.separator).getPath
        val directory: File = new File(testClassPath + ".." + File.separator + "target" + File.separator)
        base = MessageFormat.format("{0}" + File.separator + "{1}" + File.separator + "main" + File.separator,
          directory.getCanonicalPath, getFinalName)
      } catch {
        case e: IOException =>
          LOG.error("Can not get lib path, try to use absolute path. e:" + e.getMessage)
      }
    }
    val directory: File = new File(base + "" + File.separator + "container-logs" + File.separator + containerId)
    if (!directory.exists && !directory.mkdirs) {
      LOG.error("Error creating directory for container logs.")
    }
    directory
  }

  private def getUniqueFileInDirectory(directory: File, prefix: String, suffix: String): File = {
    var file: File = new File(directory, prefix + suffix)
    var i: Int = 0
    while ( {
      file.exists
    }) {
      LOG.info(file + " exists, incrementing")
      file = new File(directory, prefix + "_" + {
        i += 1;
        i - 1
      } + suffix)
    }
    file
  }

  def dumpContainerLogDirToTarget(docker: DockerClient, containerId: String, path: String): Unit = {
    val targetDirectory: File = getTargetDirectory(containerId)
    try {
      val dockerStream: InputStream = docker.copyArchiveFromContainerCmd(containerId, path).exec
      val stream: TarArchiveInputStream = new TarArchiveInputStream(dockerStream)
      try {
        var entry: TarArchiveEntry = stream.getNextTarEntry
        while (entry != null) {
          if (entry.isFile) {
            val output: File = new File(targetDirectory, entry.getName.replace(File.separator, "-"))
            Files.copy(stream, output.toPath, StandardCopyOption.REPLACE_EXISTING)
          }
          entry = stream.getNextTarEntry
        }
      } catch {
        case e@(_: RuntimeException | _: IOException) =>
          LOG.error("Error reading logs from container " + containerId, e)
      } finally {
        if (dockerStream != null) dockerStream.close()
        if (stream != null) stream.close()
      }
    }
  }

  def getContainerIP(docker: DockerClient, containerId: String): String = {
    import scala.collection.JavaConversions._
    for (e <- docker.inspectContainerCmd(containerId).exec.getNetworkSettings.getNetworks.entrySet) {
      return e.getValue.getIpAddress
    }
    throw new IllegalArgumentException("Container " + containerId + " has no networks")
  }

  @throws[ContainerExecException]
  @throws[ExecutionException]
  @throws[InterruptedException]
  def runCommand(docker: DockerClient, containerId: String, cmd: Seq[String]): ContainerExecResult = {
    try return runCommandAsync(docker, containerId, cmd).get
    catch {
      case e: ExecutionException =>
        e.getCause match {
          case exception: ContainerExecException =>
            throw exception
          case _ =>
        }
        throw e
    }
  }

  def runCommandAsync(dockerClient: DockerClient, containerId: String, cmd: Seq[String]): CompletableFuture[ContainerExecResult] = {
    val execId: String = dockerClient.execCreateCmd(containerId).withCmd(cmd: _*).withAttachStderr(true).withAttachStdout(true).exec.getId
    runCommandAsync(execId, dockerClient, containerId, cmd)
  }

  @throws[ContainerExecException]
  @throws[ExecutionException]
  @throws[InterruptedException]
  def runCommandAsUser(userId: String, docker: DockerClient, containerId: String, cmd: Seq[String]): ContainerExecResult = {
    try {
      return runCommandAsyncAsUser(userId, docker, containerId, cmd).get
    }
    catch {
      case e: ExecutionException =>
        e.getCause match {
          case exception: ContainerExecException =>
            throw exception
          case _ =>
        }
        throw e
    }
  }

  def runCommandAsyncAsUser(userId: String, dockerClient: DockerClient, containerId: String, cmd: Seq[String]): CompletableFuture[ContainerExecResult] = {
    val execId: String = dockerClient.execCreateCmd(containerId).withCmd(cmd: _*).withAttachStderr(true).withAttachStdout(true).withUser(userId).exec.getId
    return runCommandAsync(execId, dockerClient, containerId, cmd)
  }

  private def runCommandAsync(execId: String, dockerClient: DockerClient, containerId: String, cmd: Seq[String]): CompletableFuture[ContainerExecResult] = {
    val future: CompletableFuture[ContainerExecResult] = new CompletableFuture[ContainerExecResult]
    val containerName: String = getContainerName(dockerClient, containerId)
    val cmdString: String = cmd.mkString(" ")
    val stdout: StringBuilder = new StringBuilder
    val stderr: StringBuilder = new StringBuilder
    dockerClient.execStartCmd(execId).withDetach(false).exec(new ResultCallback[Frame]() {
      override def close(): Unit = {
      }

      override def onStart(closeable: Closeable): Unit = {
        LOG.info("DOCKER.exec(" + containerName + ":" + cmdString + "): Executing...")
      }

      override def onNext(`object`: Frame): Unit = {
        LOG.info("DOCKER.exec(" + containerName + ":" + cmdString + "): " + `object`)
        if (StreamType.STDOUT eq `object`.getStreamType) {
          stdout.append(new String(`object`.getPayload, UTF_8))
        }
        else {
          if (StreamType.STDERR eq `object`.getStreamType) {
            stderr.append(new String(`object`.getPayload, UTF_8))
          }
        }
      }

      override def onError(throwable: Throwable): Unit = {
        future.completeExceptionally(throwable)
      }

      override def onComplete(): Unit = {
        LOG.info("DOCKER.exec(" + containerName + ":" + cmdString + "): Done")
        val resp: InspectExecResponse = waitForExecCmdToFinish(dockerClient, execId)
        val retCode: Long = resp.getExitCodeLong
        val result: ContainerExecResult = ContainerExecResult.of(retCode, stdout.toString, stderr.toString)
        LOG.info(s"DOCKER.exec($containerName:$cmdString): completed with $retCode")
        if (retCode != 0) {
          LOG.error(s"DOCKER.exec($containerName:$cmdString): completed with non zero return code: ${result.getExitCode}\nstdout: ${result.getStdout}\nstderr:" + " ${result.getStderr}")
          future.completeExceptionally(new ContainerExecException(cmdString, containerId, result))
        }
        else {
          future.complete(result)
        }
      }
    })
    future
  }

  @throws[ContainerExecException]
  def runCommandWithRawOutput(dockerClient: DockerClient, containerId: String, cmd: Seq[String]): ContainerExecResultBytes = {
    val future: CompletableFuture[Boolean] = new CompletableFuture[Boolean]
    val execId: String = dockerClient.execCreateCmd(containerId).withCmd(cmd: _*).withAttachStderr(true).withAttachStdout(true).exec.getId
    val containerName: String = getContainerName(dockerClient, containerId)
    val cmdString: String = cmd.mkString(" ")
    val stdout: ByteArrayOutputStream = new ByteArrayOutputStream
    val stderr: ByteArrayOutputStream = new ByteArrayOutputStream
    dockerClient.execStartCmd(execId).withDetach(false).exec(new ResultCallback[Frame]() {
      override def close(): Unit = {
      }

      override def onStart(closeable: Closeable): Unit = {
        LOG.info(s"DOCKER.exec($containerName:$cmdString): Executing...")
      }

      override def onNext(`object`: Frame): Unit = {
        try if (StreamType.STDOUT eq `object`.getStreamType) {
          stdout.write(`object`.getPayload)
        }
        else {
          if (StreamType.STDERR eq `object`.getStreamType) {
            stderr.write(`object`.getPayload)
          }
        }
        catch {
          case e: IOException =>
            throw new UncheckedIOException(e)
        }
      }

      override def onError(throwable: Throwable): Unit = {
        future.completeExceptionally(throwable)
      }

      override def onComplete(): Unit = {
        LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): Done")
        future.complete(true)
      }
    })
    future.join
    val resp: InspectExecResponse = waitForExecCmdToFinish(dockerClient, execId)
    val retCode: Long = resp.getExitCodeLong
    val result: ContainerExecResultBytes = ContainerExecResultBytes.of(retCode, stdout.toByteArray, stderr.toByteArray)
    LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): completed with ${retCode}")
    if (retCode != 0) {
      throw new ContainerExecException(cmdString, containerId, null)
    }
    return result
  }

  def runCommandAsyncWithLogging(dockerClient: DockerClient, containerId: String, cmd: Seq[String]): CompletableFuture[Long] = {
    val future: CompletableFuture[Long] = new CompletableFuture[Long]
    val execId: String = dockerClient.execCreateCmd(containerId).withCmd(cmd: _*).withAttachStderr(true).withAttachStdout(true).exec.getId
    val containerName: String = getContainerName(dockerClient, containerId)
    val cmdString: String = cmd.mkString(" ")
    dockerClient.execStartCmd(execId).withDetach(false).exec(new ResultCallback[Frame]() {
      override def close(): Unit = {
      }

      override def onStart(closeable: Closeable): Unit = {
        LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): Executing...")
      }

      override def onNext(`object`: Frame): Unit = {
        LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): ${`object`}")
      }

      override def onError(throwable: Throwable): Unit = {
        future.completeExceptionally(throwable)
      }

      override def onComplete(): Unit = {
        LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): Done")
        val resp: InspectExecResponse = waitForExecCmdToFinish(dockerClient, execId)
        val retCode: Long = resp.getExitCodeLong
        LOG.info(s"DOCKER.exec(${containerName}:${cmdString}): completed with ${retCode}")
        future.complete(retCode)
      }
    })
    return future
  }

  private def getContainerName(dockerClient: DockerClient, containerId: String): String = {
    val inspectContainerResponse: InspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec
    // docker api returns names prefixed with File.separator, it's part of it's legacy design,
    // this removes it to be consistent with what docker ps shows.
    inspectContainerResponse.getName.replace(File.separator, "")
  }

  private def waitForExecCmdToFinish(dockerClient: DockerClient, execId: String): InspectExecResponse = {
    var resp: InspectExecResponse = dockerClient.inspectExecCmd(execId).exec
    while ( {
      resp.isRunning
    }) {
      try Thread.sleep(200)
      catch {
        case ie: InterruptedException =>
          Thread.currentThread.interrupt()
          throw new RuntimeException(ie)
      }
      resp = dockerClient.inspectExecCmd(execId).exec
    }
    return resp
  }

  def getContainerCluster(docker: DockerClient, containerId: String): Optional[String] = {
    return Optional.ofNullable(docker.inspectContainerCmd(containerId).exec.getConfig.getLabels.get("cluster"))
  }
}