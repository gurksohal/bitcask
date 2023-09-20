package bitcask;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskManager {
    private static final int FILE_SIZE_THRESHOLD = 16_000;
    private final String dbDir;
    private File openFile;

    public DiskManager(String dbDir) throws IOException {
        this.dbDir = dbDir;
        if (createDir(dbDir)) {
            System.out.println("Created DB dir: " + dbDir);
        }
        this.openFile = createFile();
    }

    /**
     * return[0] = fileName
     * return[1] = offset to value
     */
    public String[] writeRecord(Record r) throws IOException {
        if (openFile.length() >= FILE_SIZE_THRESHOLD) {
            this.openFile = createFile();
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(r.toBytes());
        // current file size + timestamp (8 bytes) + keySize (4 bytes) + valueSize (4 bytes) + key value
        int valueOffset = (int) openFile.length() + 4 + 8 + 4 + 4 + r.getKeySize();

        try (FileOutputStream out = new FileOutputStream(openFile, true)) {
            out.write(ByteUtils.intToBytes(outputStream.size()));
            out.write(outputStream.toByteArray());
            out.flush();
            out.getFD().sync();
        }

        return new String[] {openFile.getAbsolutePath(), String.valueOf(valueOffset)};
    }

    public byte[] readBytes(String fileName, int offset, int size) throws IOException {
        byte[] bytes = new byte[size];
        try (RandomAccessFile file = new RandomAccessFile(new File(fileName), "r")) {
            file.seek(offset);
            file.read(bytes, 0, size);
        }

        return bytes;
    }

    private boolean createDir(String dirName) {
        File file = new File(dirName);
        return file.mkdirs();
    }

    private File createFile() throws IOException {
        File file = new File(dbDir,"BITCASK_" + System.currentTimeMillis());
        try (FileOutputStream out = new FileOutputStream(file)) {
            out.getFD().sync();
        }

        return file;
    }
}
