package bitcask;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BitcaskDB {
    private static final String TOMBSTONE_VALUE = "TOMBSTONE";
    private final Map<String, RecordMetadata> keyDir;
    private final String databaseDir;
    private final DiskManager diskManager;

    public BitcaskDB(String databaseDir) throws IOException {
        this.databaseDir = databaseDir;
        this.diskManager = new DiskManager(databaseDir);
        keyDir = new HashMap<>();
        buildKeyDir();
    }

    public String get(String key) throws IOException {
        if (!keyDir.containsKey(key)) {
            System.out.println(key + " is not in db");
            return null;
        }

        RecordMetadata recordMetadata = keyDir.get(key);
        byte[] bytes = diskManager.readBytes(recordMetadata.getFileName(), recordMetadata.getValuePos(), recordMetadata.getValueSize());
        return new String(bytes);
    }

    public void put(String key, String value) throws IOException {
        Record r = new Record(key, value);
        String[] writeRes = diskManager.writeRecord(r);
        int valuePos = Integer.parseInt(writeRes[1]);
        keyDir.put(key, new RecordMetadata(writeRes[0], r, valuePos));
    }

    public void delete(String key) throws IOException {
        if (!keyDir.containsKey(key)) {
            System.out.println(key + " is not in db");
            return;
        }

        Record r = new Record(key, TOMBSTONE_VALUE);
        diskManager.writeRecord(r);
        keyDir.remove(key);
    }

    private void buildKeyDir() throws IOException {
        File dir = new File(databaseDir);
        File[] files = dir.listFiles();
        if (files == null) return;

        Arrays.sort(files, (f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()));
        for (File file : files) {
            processFile(file);
        }
    }

    private void processFile(File file) throws IOException {
        byte[] bytes = new byte[(int) file.length()];
        try (FileInputStream inputStream = new FileInputStream(file)) {
            inputStream.read(bytes);
        }

        int currOffset = 0;
        while (currOffset < bytes.length) {
            int recordSize = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, currOffset, currOffset+4));
            currOffset += 4;
            byte[] recordBytes = Arrays.copyOfRange(bytes, currOffset, currOffset+recordSize);

            Record r = Record.fromBytes(recordBytes);
            // current file size + timestamp (8 bytes) + keySize (4 bytes) + valueSize (4 bytes) + key value
            int valueOffset = currOffset + 8 + 4 + 4 + r.getKeySize();
            currOffset += recordSize;

            if (r.getValue().equals(TOMBSTONE_VALUE)) {
                continue;
            }

            if (!keyDir.containsKey(r.getKey()) || keyDir.get(r.getKey()).getFileName().equals(file.getAbsolutePath())) {
                keyDir.put(r.getKey(), new RecordMetadata(file.getAbsolutePath(), r, valueOffset));
            }
        }
    }
}
