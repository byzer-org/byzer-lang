package streaming.common;

import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import java.io.IOException;
import java.net.URI;

/**
 * Created by allwefantasy on 27/8/2018.
 */
public class JavaStringObject extends SimpleJavaFileObject {
    private final String source;

    protected JavaStringObject(String name, String source) {
        super(URI.create("string:///" + name.replaceAll("\\.", "/") +
                JavaFileObject.Kind.SOURCE.extension), JavaFileObject.Kind.SOURCE);
        this.source = source;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors)
            throws IOException {
        return source;
    }
}
