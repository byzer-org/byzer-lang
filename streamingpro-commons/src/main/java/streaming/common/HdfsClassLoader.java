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

package streaming.common;

/**
 * Created by allwefantasy on 20/9/2017.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * A {@link ClassLoader} that reads its class data from a jar file stored in HDFS.
 *
 * @author jeff@opower.com
 */
public class HdfsClassLoader extends ClassLoader {
    /**
     * A configuration option to indicate whether or not to skip the standard {@link ClassLoader} hierarchical
     * search and look at the configured jar first.  Defaults to {@code false} so that the standard search is performed,
     * but it may be helpful to turn to {@code true} for debugging.
     */
    public static final String ATTEMPT_LOCAL_LOAD_FIRST = "hdfs.classloader.attemptLocalFirst";

    private static final Log LOG = LogFactory.getLog(HdfsClassLoader.class);

    private final Configuration configuration;
    private final Path jar;

    /**
     * @param configuration The Hadoop configuration to use to read from HDFS
     * @param jar A path to a jar file containing classes to load
     */
    public HdfsClassLoader(Configuration configuration, Path jar) {
        super(HdfsClassLoader.class.getClassLoader());
        this.configuration = configuration;
        this.jar = jar;
    }

    /**
     * Override to allow for checking the local jar first instead of the standard search which would check
     * the parent class loader first.
     *
     * {@inheritDoc}
     */
    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        if (this.configuration.getBoolean(ATTEMPT_LOCAL_LOAD_FIRST, false)) {
            try {
                return findClass(className);
            }
            catch (ClassNotFoundException cnfe) {
                // This exception can be ignored, because the standard
                // case will be attempted below
            }
        }

        // try the standard approach
        return super.loadClass(className);
    }

    /**
     * Search for the class in the configured jar file stored in HDFS.
     *
     * {@inheritDoc}
     */
    @Override
    public Class findClass(String className) throws ClassNotFoundException {
        String classPath = convertNameToPath(className);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Searching for class %s (%s) in path %s", className, classPath, this.jar));
        }
        FileSystem fs = null;
        JarInputStream jarIn = null;
        try {
            fs = this.jar.getFileSystem(this.configuration);
            jarIn = new JarInputStream(fs.open(this.jar));
            JarEntry currentEntry = null;
            while ((currentEntry = jarIn.getNextJarEntry()) != null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("Comparing %s to entry %s", classPath, currentEntry.getName()));
                }
                if (currentEntry.getName().equals(classPath)) {
                    byte[] classBytes = readEntry(jarIn);
                    return defineClass(className, classBytes, 0, classBytes.length);
                }
            }
        }
        catch (IOException ioe) {
            throw new ClassNotFoundException(
                    "Unable to find " + className + " in path " + this.jar, ioe);
        }
        finally {
            closeQuietly(jarIn);
            // While you would think it would be prudent to close the filesystem that you opened,
            // it turns out that this filesystem is shared with HBase, so when you close this one,
            // it becomes closed for HBase, too.  Therefore, there is no call to closeQuietly(fs);
        }
        throw new ClassNotFoundException("Unable to find " + className + " in path " + this.jar);
    }

    /**
     * Converts a binary class name to the path that it would show up as in a jar file.
     * For example, java.lang.String would show up as a jar entry at java/lang/String.class
     */
    private String convertNameToPath(String className) {
        String classPath = className.replace('.', '/');
        classPath += ".class";
        return classPath;
    }

    /**
     * Read an entry out of the jar file
     */
    private byte[] readEntry(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) > 0) {
            out.write(buffer, 0, read);
        }
        out.flush();
        out.close();
        return out.toByteArray();
    }

    /**
     * Close the {@link Closeable} without any exceptions
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ioe) {
                // the point of being quiet is to not propogate this
            }
        }
    }
}

