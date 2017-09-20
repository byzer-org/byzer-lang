package streaming.common

import java.io.File
import java.net.{URI, URL, URLClassLoader}


/**
  * Created by allwefantasy on 20/9/2017.
  */
object JarUtil {
  def loadJar(path: String) = {
    val uri = new URI(path).toURL
    val classLoader = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(classLoader, uri)
  }
}

