package streaming.core;

import org.apache.hadoop.fs.FileStatus;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarHeader;

public class HDFSTarEntry extends TarEntry {
    private FileStatus hdfsFileStatus;

    public HDFSTarEntry(FileStatus hdfsFileStatus, String entryName) {
        super(null, entryName);
        this.hdfsFileStatus = hdfsFileStatus;
        header = TarHeader.createHeader(entryName, hdfsFileStatus.getLen(), hdfsFileStatus.getModificationTime() / 1000, hdfsFileStatus.isDirectory());
    }

    @Override
    public void extractTarHeader(String entryName) {
    }
}
