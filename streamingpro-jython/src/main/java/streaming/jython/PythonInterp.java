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

    public static void main(String[] args) {
        String source = "class A:\n" +
                "\tdef a(self,k1,k2):\n" +
                "\t    return k1 + k2";
        PyObject ob = compilePython(source, "A");
        PyObject instance = ob.__call__();
        System.out.println(instance.__getattr__("a").__call__(new PyInteger(2), new PyInteger(3)));
    }

}
