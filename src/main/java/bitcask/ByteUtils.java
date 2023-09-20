package bitcask;

import java.nio.ByteBuffer;

public class ByteUtils {
    private static final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private static final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);

    private ByteUtils() {}

    public static byte[] longToBytes(long x) {
        longBuffer.clear();
        longBuffer.putLong(0, x);
        return longBuffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        longBuffer.clear();
        longBuffer.put(bytes, 0, bytes.length);
        longBuffer.flip();
        return longBuffer.getLong();
    }

    public static byte[] intToBytes(int x) {
        intBuffer.clear();
        intBuffer.putInt(x);
        return intBuffer.array();
    }

    public static int bytesToInt(byte[] bytes) {
        intBuffer.clear();
        intBuffer.put(bytes, 0, bytes.length);
        intBuffer.flip();
        return intBuffer.getInt();
    }

}