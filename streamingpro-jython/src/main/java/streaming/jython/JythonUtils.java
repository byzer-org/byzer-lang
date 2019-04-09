/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.jython;

import org.python.core.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by allwefantasy on 28/8/2018.
 */
public class JythonUtils {
    public static Object toJava(PyObject pyObject) {
        try {
            Object javaObj = null;
            // Add code for all supported pig types here
            // Tuple, bag, map, int, long, float, double, chararray, bytearray
            if (pyObject instanceof PyTuple) {
                PyTuple pyTuple = (PyTuple) pyObject;
                Object[] tuple = new Object[pyTuple.size()];
                int i = 0;
                for (PyObject tupleObject : pyTuple.getArray()) {
                    tuple[i++] = toJava(tupleObject);
                }
                javaObj = Arrays.asList(tuple);
            } else if (pyObject instanceof PyList) {
                List list = new ArrayList();
                for (PyObject bagTuple : ((PyList) pyObject).asIterable()) {
                    // In jython, list need not be a bag of tuples, as it is in case of pig
                    // So we fail with cast exception if we dont find tuples inside bag
                    // This is consistent with java udf (bag should be filled with tuples)
                    list.add(toJava(bagTuple));
                }
                javaObj = list;
            } else if (pyObject instanceof PyDictionary) {
                Map<?, Object> map = Py.tojava(pyObject, Map.class);
                Map<Object, Object> newMap = new HashMap<Object, Object>();
                for (Map.Entry<?, Object> entry : map.entrySet()) {
                    if (entry.getValue() instanceof PyObject) {
                        newMap.put(entry.getKey(), toJava((PyObject) entry.getValue()));
                    } else {
                        // Jython sometimes uses directly the java class: for example for integers
                        newMap.put(entry.getKey(), entry.getValue());
                    }
                }
                javaObj = newMap;
            } else if (pyObject instanceof PyLong) {
                javaObj = pyObject.__tojava__(Long.class);
            } else if (pyObject instanceof PyInteger) {
                javaObj = pyObject.__tojava__(Integer.class);
            } else if (pyObject instanceof PyFloat) {
                // J(P)ython is loosely typed, supports only float type,
                // hence we convert everything to double to save precision
                javaObj = pyObject.__tojava__(Double.class);
            } else if (pyObject instanceof PyString) {
                javaObj = pyObject.__tojava__(String.class);
            } else if (pyObject instanceof PyBoolean) {
                javaObj = pyObject.__tojava__(Boolean.class);
            } else if (pyObject instanceof PyNone) {
                return null;
            } else {
                javaObj = pyObject.__tojava__(byte[].class);
                // if we successfully converted to byte[]
                if (javaObj instanceof byte[]) {

                } else {
                    throw new RuntimeException("Non supported pig datatype found, cast failed: " + (pyObject == null ? null : pyObject.getClass().getName()));
                }
            }
            if (javaObj.equals(Py.NoConversion)) {
                throw new RuntimeException("Cannot cast into any pig supported type: " + (pyObject == null ? null : pyObject.getClass().getName()));
            }
            return javaObj;
        } catch (Exception e) {
            throw new RuntimeException("Cannot convert jython type (" + (pyObject == null ? null : pyObject.getClass().getName()) + ") to pig datatype " + e, e);
        }
    }

    public static PyObject toPy(Object o) {

        if (o instanceof List) {
            Iterator<PyObject> converter = ((List<Object>) o).stream()
                    .map(JythonUtils::toPy)
                    .iterator();
            return new PyList(converter);
        } else if (o instanceof Seq) {
            Collection converter = JavaConverters.asJavaCollectionConverter((Seq) o).asJavaCollection();
            return toPy(new ArrayList(converter));
        } else if (o instanceof scala.collection.immutable.Map) {
            Map converter = (Map) JavaConverters.mapAsJavaMapConverter((scala.collection.immutable.Map) o).asJava();
            return toPy(converter);
        } else if (o instanceof Map) {
            PyDictionary dictionary = new PyDictionary();
            ((Map<Object, Object>) o).entrySet().stream()
                    .map(v -> new AbstractMap.SimpleEntry<>(toPy(v.getKey()), toPy(v.getValue())))
                    .forEach(v -> dictionary.put(v.getKey(), v.getValue()));
            return dictionary;
        } else if (o instanceof Integer) {
            return new PyInteger((int) o);
        } else if (o instanceof Long) {
            return new PyLong((long) o);
        } else if (o instanceof Float) {
            return new PyFloat((float) o);
        } else if (o instanceof Double) {
            return new PyFloat((double) o);
        } else if (o instanceof Boolean) {
            return new PyBoolean((boolean) o);
        } else if (o == null) {
            return Py.None;
        } else if (o instanceof Character) {
            return Py.newStringOrUnicode(((Character) o).toString());
        } else if (o instanceof String) {
            return Py.newStringOrUnicode((String) o);
        } else {
            throw new RuntimeException(o.getClass().getCanonicalName() + " can not been converted to python object");
        }
    }
}
