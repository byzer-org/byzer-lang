package streaming.udf

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.reflect.ClassPath
import streaming.dsl.mmlib.algs.ScriptUDFCacheKey
import streaming.log.Logging

/**
 * Created by fchen on 2018/11/13.
 */
trait RuntimeCompileScriptInterface[FunType] extends Logging {

  private val _scriptCache = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .build(
      new CacheLoader[ScriptUDFCacheKey, AnyRef]() {
        override def load(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
          val startTime = System.nanoTime()
          val compiled = compile(scriptCacheKey)

          def timeMs: Double = (System.nanoTime() - startTime).toDouble / 1000000

          logInfo(s"generate udf time: [ ${timeMs} ]ms.")
          compiled
        }
      })


  /**
   * validate the source code
   */
  def check(sourceCode: String): Boolean

  /**
   * how to compile the language source code with jvm.
   *
   * @param scriptCacheKey
   * @return
   */
  def compile(scriptCacheKey: ScriptUDFCacheKey): AnyRef

  /**
   * generate udf or udaf
   */
  def generateFunction(scriptCacheKey: ScriptUDFCacheKey): FunType

  def lang: String

  /**
   * compile source code or get binary code for cache.
   * @param scriptCacheKey
   * @return
   */
  def execute(scriptCacheKey: ScriptUDFCacheKey): AnyRef = {
    _scriptCache.get(scriptCacheKey)
  }
}

object RuntimeCompileScriptFactory {

  private val _udfCache = HashMap[String, RuntimeCompileUDF]()
  private val _udafCache = HashMap[String, RuntimeCompileUDAF]()
  private val _loaded = new AtomicBoolean(false)
  private val _lock = new Object()

  def getUDFCompilerBylang(lang: String): Option[RuntimeCompileUDF] = {
    if (!_loaded.get()) {
      loadAll()
    }
    _udfCache.get(lang)
  }

  def getUDAFCompilerBylang(lang: String): Option[RuntimeCompileUDAF] = {
    if (!_loaded.get()) {
      loadAll()
    }
    _udafCache.get(lang)
  }

  def registerUDF(lang: String, runtimeCompileUDF: RuntimeCompileUDF): Unit = {
    _udfCache.put(lang, runtimeCompileUDF)
  }

  def registerUDAF(lang: String, runtimeCompileUDAF: RuntimeCompileUDAF): Unit = {
    _udafCache.put(lang, runtimeCompileUDAF)
  }

  /**
   * load all [[RuntimeCompileUDF]] and [[RuntimeCompileUDAF]]
   */
  def loadAll(): Unit = {
    _lock.synchronized {
      ClassPath.from(this.getClass.getClassLoader)
        .getTopLevelClasses("streaming.udf")
        .map(_.load())
          .filter(n => {
            n.getName.endsWith("RuntimeCompileUDF") && n.getName != "streaming.udf.RuntimeCompileUDF"
          }).map(getInstance)
        .foreach {
          case udf: RuntimeCompileUDF => registerUDF(udf.lang, udf)
          case udaf: RuntimeCompileUDAF => registerUDAF(udaf.lang, udaf)
        }
      _loaded.set(true)
    }
  }

  private def getInstance(clz: Class[_]): Any = {
    import scala.reflect.runtime.universe
    val runtimeMirror = universe.runtimeMirror(this.getClass.getClassLoader)
    val module = runtimeMirror.staticModule(clz.getName)
    val obj = runtimeMirror.reflectModule(module)
    obj.instance
  }

}
