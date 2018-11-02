package testing;

import org.junit.Before;
import org.junit.Test;
import server.app.KVServer;
import server.storage.IPersistenceManager;
import server.storage.PersistenceManager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertTrue;
import server.storage.IPersistenceManager.OpStatus;

public class PersistenceTest {
    PersistenceManager persistenceManager;
    private final String key = "Some\"Key=09";
//    private String value = "==Abc\n09$8";
    private String value = "==Abc09$8";

    @Before
    public void init() {
        persistenceManager = new PersistenceManager();
    }

    @Test
    public void testCRUDFile() {
        testCreateFile();
        testReadFile();
        testUpdateFile();
        testDeleteFile();
    }

    private void testDeleteFile() {
        OpStatus status = persistenceManager.delete(key);
        assertThat(status, is(OpStatus.DELETE_SUCCESS));

        Path filePath = persistenceManager.getFilePath(key);
        assertThat(!Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
    }

    private void testUpdateFile() {
//        String newValue = "New\"Key=10\r";
        String newValue = "New\"Key=10";
        OpStatus status =  persistenceManager.write(key, newValue);
        assertThat(status, is(OpStatus.WRITE_SUCCESS));

        String get = persistenceManager.read(key);
        assertThat(get, equalTo(newValue));
    }

    private void testReadFile() {
        String get = persistenceManager.read(key);
        assertThat(get, equalTo(value));
    }

    private void testCreateFile() {
        OpStatus status =  persistenceManager.write(key, value);
        assertThat(status, is(OpStatus.WRITE_SUCCESS));
        Path filePath = persistenceManager.getFilePath(key);
        assertThat(Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
    }
}
