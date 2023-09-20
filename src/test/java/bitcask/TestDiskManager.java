package bitcask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestDiskManager {
    private static final String DIR = "testingDir";
    private static DiskManager diskManager = null;
    private static final Record r = new Record("key", "value");

    @BeforeEach
    public void beforeEach() throws IOException {
        diskManager = new DiskManager(DIR);
    }

    @AfterEach
    public void afterEach() {
        Arrays.stream(new File(DIR).listFiles()).forEach(File::delete);
        new File(DIR).delete();
    }

    @Test
    void testWriteRecord() throws IOException {
        // 4(total size) + 8(time) + 4(key size) + 4(value size) + 3(key) + 5(value) = 28 bytes
        diskManager.writeRecord(r);

        File[] files = new File(DIR).listFiles();
        assertEquals(1, Objects.requireNonNull(files).length);
        File f = files[0];
        assertEquals(28, Files.readAllBytes(Path.of(f.getAbsolutePath())).length);
    }

    @Test
    void testReadBytes() throws IOException {
        File[] files = new File(DIR).listFiles();
        for(File fileNames : files) System.out.println(fileNames);
        assertEquals(1, Objects.requireNonNull(files).length);

        File f = files[0];
        FileWriter writer = new FileWriter(f);
        writer.write("Record Test"); // 11 bytes
        writer.flush();
        writer.close();

        byte[] b = diskManager.readBytes(f.getAbsolutePath(), 0, 11);
        assertEquals("Record Test", new String(b));
    }
}
