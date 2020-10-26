package tech.mlsql.nativelib.runtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.sun.jna.NativeLibrary;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.mlsql.common.utils.path.PathFun;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

/**
 * 19/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
public class JniUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JniUtils.class);
    private static Set<String> loadedLibs = Sets.newHashSet();

    public static final String MLSQL_NATIVE_LIB = "MlsqlNativeLib";

    public static synchronized void loadLibrary(String libraryName) {
        try {
            loadLibrary(libraryName, true, null);
        } catch (Exception e) {
            LOGGER.info("Fail to loading native library {}.", libraryName);
        }

    }
    
    public static synchronized void loadLibrary(String libraryName, boolean exportSymbols, String testPath) {
        if (!loadedLibs.contains(libraryName)) {
            LOGGER.info("Loading native library {}.", libraryName);
            // Load native library.
            String fileName = System.mapLibraryName(libraryName);
            if (testPath != null) {
                PathFun pf = new PathFun(testPath);
                fileName = pf.add(fileName).toPath();
            }
            final String sessionDir = "./native/";
            final File file = getNativeFile(sessionDir, fileName);

            if (exportSymbols) {
                // Expose library symbols using RTLD_GLOBAL which may be depended by other shared
                // libraries.
                NativeLibrary.getInstance(file.getAbsolutePath());
            }
            System.load(file.getAbsolutePath());
            LOGGER.info("Native library loaded.");
            loadedLibs.add(libraryName);
        }
    }


    public static File getNativeFile(String destDir, String fileName) {
        if (fileName.startsWith("/")) {
            return new File(fileName);
        }
        final File dir = new File(destDir);
        if (!dir.exists()) {
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                throw new RuntimeException("Couldn't make directory: " + dir.getAbsolutePath(), e);
            }
        }
        String lockFilePath = destDir + File.separator + "file_lock";
        try (FileLock ignored = new RandomAccessFile(lockFilePath, "rw")
                .getChannel().lock()) {
            String resourceDir;
            if (SystemUtils.IS_OS_MAC) {
                resourceDir = "native/darwin/";
            } else if (SystemUtils.IS_OS_LINUX) {
                resourceDir = "native/linux/";
            } else {
                throw new UnsupportedOperationException("Unsupported os " + SystemUtils.OS_NAME);
            }
            String resourcePath = resourceDir + fileName;
            File file = new File(String.format("%s/%s", destDir, fileName));
            if (file.exists()) {
                return file;
            }

            // File does not exist.
            try (InputStream is = JniUtils.class.getResourceAsStream("/" + resourcePath)) {
                Preconditions.checkNotNull(is, "{} doesn't exist.", resourcePath);
                Files.copy(is, Paths.get(file.getCanonicalPath()));
            } catch (IOException e) {
                throw new RuntimeException("Couldn't get temp file from resource " + resourcePath, e);
            }
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
