package bitcask;

public class RecordMetadata {
    private final String fileName;
    private final int valueSize;
    private final int valuePos;
    private final long timestamp;

    public RecordMetadata(String fileName, Record r, int valuePos) {
        this.fileName = fileName;
        this.valueSize = r.getValueSize();
        this.valuePos = valuePos;
        this.timestamp = r.getTimestamp();
    }

    public String getFileName() {
        return fileName;
    }

    public int getValueSize() {
        return valueSize;
    }

    public int getValuePos() {
        return valuePos;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
