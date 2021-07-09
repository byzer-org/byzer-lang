package tech.mlsql.tool;


import java.io.*;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;


public class DecompressUtil {
    final private static String[] archiveFileType = {".tar", ".tar.gz", ".gz", ".zip"};

    public static String parseFileNamePrefix(String fileName, String fileType) {
        return fileName.substring(0, fileName.length() - fileType.length());
    }

    public static Boolean isArchiveFile(String fileName) {
        for (String fileType : archiveFileType) {
            if (fileName.endsWith(fileType)) {
                return true;
            }
        }
        return false;
    }

    public static void decompressAndSaveStream(
            String path, String fileName, InputStream inputStream
    ) throws IOException {
        String matchedType = null;
        for (String fType : archiveFileType) {
            if (fileName.endsWith(fType)) {
                matchedType = fType;
                break;
            }
        }
        if (matchedType == null) {
            throw new IOException(String.format("Unable to unarchive file [%s].", fileName));
        }
        String dirPath = new File(path, parseFileNamePrefix(fileName, matchedType)).getPath();
        switch (matchedType) {
            case ".zip":
                decompressZipFile(dirPath, inputStream);
                break;
            case ".tar":
                decompressTarFile(dirPath, inputStream);
                break;
            case ".gz":
                decompressGzFile(dirPath, inputStream, fileName);
                break;
            case ".tar.gz":
                decompressTarGzFile(dirPath, inputStream);
                break;
        }
    }

    public static void decompressZipFile(String path, InputStream inputStream) throws IOException {
        try (ZipArchiveInputStream ZipIS = new ZipArchiveInputStream(inputStream)) {
            decompressArchiveInputStream(path, ZipIS);
        }
    }

    public static void decompressTarFile(String path, InputStream inputStream) throws IOException {
        try (TarArchiveInputStream tarIS = new TarArchiveInputStream(inputStream)) {
            decompressArchiveInputStream(path, tarIS);
        }
    }

    public static void decompressArchiveInputStream(String path, ArchiveInputStream archiveIS) throws IOException {
        ArchiveEntry entry;
        while ((entry = archiveIS.getNextEntry()) != null) {
            String fullName = entry.getName();
            File file = new File(path, fullName);
            if (entry.isDirectory()) {
                HDFSOperatorV2.createDir(file.getPath());
            } else {
                HDFSOperatorV2.saveStream(
                        file.getParent(),
                        file.getName(),
                        archiveIS
                );
            }
        }
    }

    public static void decompressGzFile(String path, InputStream inputStream, String fileName) throws IOException {
        try (GzipCompressorInputStream gzipIS = new GzipCompressorInputStream(inputStream)) {
            HDFSOperatorV2.saveStream(path, parseFileNamePrefix(fileName, ".gz"), gzipIS);
        }

    }

    public static void decompressTarGzFile(String path, InputStream inputStream) throws IOException {
        try (GzipCompressorInputStream gzipIS = new GzipCompressorInputStream(inputStream);
             TarArchiveInputStream tarIS = new TarArchiveInputStream(gzipIS)
        ) {
            decompressArchiveInputStream(path, tarIS);
        }
    }
}

