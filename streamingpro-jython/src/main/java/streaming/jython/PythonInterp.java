package streaming.jython;

import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import java.util.Properties;

/**
 * Created by allwefantasy on 28/8/2018.
 */
public class PythonInterp {

    private static PythonInterpreter pi = null;

    static {
        Properties props = new Properties();
        props.put("python.import.site", "false");
        Properties preprops = System.getProperties();
        PythonInterpreter.initialize(preprops, props, new String[0]);
        pi = new PythonInterpreter();
    }

    public static PyObject compilePython(String src, String methodName) {
        pi.exec(src);
        return pi.get(methodName);
    }

}
