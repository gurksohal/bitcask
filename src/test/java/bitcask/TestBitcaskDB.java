package bitcask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestBitcaskDB {
    private static final String DIR = "testingDir";
    private static BitcaskDB db = null;

    @BeforeEach
    public void beforeEach() throws IOException {
        db = new BitcaskDB(DIR);
    }

    @AfterEach
    public void afterEach() {
        Arrays.stream(new File(DIR).listFiles()).forEach(File::delete);
        new File(DIR).delete();
    }

    @Test
    void testPutAndGet() throws IOException {
        String key = "key";
        String val = "val";
        db.put(key, val);

        String returnedVal = db.get(key);
        assertEquals(val, returnedVal);
    }

    @Test
    void testDelete() throws IOException {
        String key = "key";
        String val = "val";
        db.put(key, val);
        db.delete(key);
        assertNull(db.get(key));
    }

    @Test
    void testManyGetAndPut() throws IOException {
        String key = "key";
        String value = "value";
        for (int i = 0; i < 100_000; i++) {
            db.put(key+i, value+i);
        }

        for (int i = 0; i < 1_000; i++) {
            int rand = ThreadLocalRandom.current().nextInt(10_000);
            String returnedVal = db.get(key+rand);
            assertEquals(value+rand, returnedVal);
        }
    }

}
