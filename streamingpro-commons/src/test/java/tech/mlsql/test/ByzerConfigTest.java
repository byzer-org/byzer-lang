package tech.mlsql.test;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import tech.mlsql.test.utils.DockerUtils;
import tech.mlsql.tool.ByzerConfigCLI;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByzerConfigTest {
    @Test
    public void testSparkConfig() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        ByzerConfigCLI.execute(new String[] { "-spark" });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        List<String> prop = Arrays.asList(val.split("\\n"));
        AtomicBoolean prefix = new AtomicBoolean(true);
        prop.forEach(p-> {
            if(!p.startsWith("--conf")) {
                prefix.set(false);
            }
        } );
        assertTrue(prefix.get());
        assertTrue(val.contains("spark.master"));
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testByzerConfig() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        ByzerConfigCLI.execute(new String[] { "-byzer" });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        List<String> prop = Arrays.asList(val.split("\\n"));
        AtomicBoolean prefix = new AtomicBoolean(true);
        prop.forEach(p-> {
            if(!p.startsWith("-")) {
                prefix.set(false);
            }
        } );
        assertTrue(prefix.get());
        assertTrue(val.contains("streaming.rest"));
        assertTrue(val.contains("streaming.platform"));
        assertTrue(val.contains("streaming.spark.service"));
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetSingleProp() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        ByzerConfigCLI.execute(new String[] { "streaming.rest" });
        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("true", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Before
    public void setUp() {
        String ByzerHomePath = DockerUtils.getRootPath();
        System.setProperty("BYZER_HOME", ByzerHomePath);
    }

}
