package bitcask;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class Record {

    private long timestamp;
    private int keySize;
    private int valueSize;
    private String key;
    private String value;

    private Record() {}

    public Record(String key, String value) {
        this.key = key;
        this.value = value;
        this.keySize = key.getBytes().length;
        this.valueSize = value.getBytes().length;
        this.timestamp = System.currentTimeMillis();
    }

    public static Record fromBytes(byte[] bytes) {
        Record r = new Record();
        r.timestamp = ByteUtils.bytesToLong(Arrays.copyOfRange(bytes, 0, 8));
        r.keySize = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, 8, 12));
        r.valueSize = ByteUtils.bytesToInt(Arrays.copyOfRange(bytes, 12, 16));
        r.key = new String(Arrays.copyOfRange(bytes, 16, r.keySize+16));
        r.value = new String(Arrays.copyOfRange(bytes, 16+r.keySize, r.valueSize+16+r.keySize));
        return r;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(ByteUtils.longToBytes(timestamp));
        outputStream.write(ByteUtils.intToBytes(keySize));
        outputStream.write(ByteUtils.intToBytes(valueSize));
        outputStream.write(key.getBytes());
        outputStream.write(value.getBytes());
        return outputStream.toByteArray();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
