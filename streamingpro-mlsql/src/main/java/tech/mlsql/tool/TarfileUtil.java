package tech.mlsql.tool;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;
import org.kamranzafar.jtar.TarOutputStream;
import streaming.core.HDFSTarEntry;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 24/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class TarfileUtil {
    private static Logger logger = Logger.getLogger(TarfileUtil.class);

    public static void walk(FileSystem fs, List<FileStatus> files, Path p) throws IOException {

        if (fs.isFile(p)) {
            files.add(fs.getFileStatus(p));
        } else if (fs.isDirectory(p)) {
            walk(fs, files, p);
        }
    }

    public static List<String> extractTarFile(InputStream inputStream) throws IOException {
        TarInputStream tarInputStream = new TarInputStream(new BufferedInputStream(inputStream));
        TarEntry entry = tarInputStream.getNextEntry();
        List<String> fileNames = new ArrayList<>();
        while (entry != null) {
            fileNames.add(entry.getName());
            entry = tarInputStream.getNextEntry();
        }
        tarInputStream.close();
        inputStream.close();
        return fileNames;
    }

    public static List<String> extractTarFileFromPath(String path) throws IOException {
        FileSystem fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration());
        FSDataInputStream fis = fs.open(new Path(path));
        return extractTarFile(fis);
    }

    public static int createTarFileStream(OutputStream output, String pathStr) throws IOException {
        FileSystem fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration());
        String[] paths = pathStr.split(",");
        try {
            OutputStream outputStream = output;

            TarOutputStream tarOutputStream = new TarOutputStream(new BufferedOutputStream(outputStream));

            List<FileStatus> files = new ArrayList<FileStatus>();

            for (String path : paths) {
                walk(fs, files, new Path(path));
            }

            if (files.size() > 0) {
                FSDataInputStream inputStream = null;
                int len = files.size();
                int i = 1;
                for (FileStatus cur : files) {
                    logger.info("[" + i++ + "/" + len + "]" + ",读取文件" + cur);
                    inputStream = fs.open(cur.getPath());

                    tarOutputStream.putNextEntry(new HDFSTarEntry(cur, cur.getPath().getName()));
                    org.apache.commons.io.IOUtils.copyLarge(inputStream, tarOutputStream);
                    inputStream.close();

                }
                tarOutputStream.flush();
                tarOutputStream.close();
                return 200;
            } else return 400;

        } catch (Exception e) {
            e.printStackTrace();
            return 500;

        }
    }
}
