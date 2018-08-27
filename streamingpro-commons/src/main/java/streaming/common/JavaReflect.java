package streaming.common;

import javax.tools.*;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by allwefantasy on 27/8/2018.
 */
public class JavaReflect {
    public static JavaFileManager createFileManager(StandardJavaFileManager fileManager,
                                                    JavaByteObject byteObject) {
        return new ForwardingJavaFileManager<StandardJavaFileManager>(fileManager) {
            @Override
            public JavaFileObject getJavaFileForOutput(Location location,
                                                       String className, JavaFileObject.Kind kind,
                                                       FileObject sibling) throws IOException {
                return byteObject;
            }
        };
    }

    public static ClassLoader createClassLoader(final JavaByteObject byteObject) {
        return new ClassLoader() {
            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                //no need to search class path, we already have byte code.
                byte[] bytes = byteObject.getBytes();
                return defineClass(name, bytes, 0, bytes.length);
            }
        };
    }

    public static Iterable<? extends JavaFileObject> getCompilationUnits(String className, String src) {
        JavaStringObject stringObject = new JavaStringObject(className, src);
        return Arrays.asList(stringObject);
    }
}
