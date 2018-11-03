package testing;

import org.junit.Before;
import org.junit.Test;
import protocol.IMessage;
import server.storage.disk.PersistentStorage;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertTrue;
import static util.StringUtils.PATH_SEP;

import server.storage.disk.IPersistentStorage.OpStatus;

public class PersistenceTest {
    String dbPath = System.getProperty("user.dir") + PATH_SEP + "test" + PATH_SEP +"db";
    PersistentStorage persistenceManager;
    private final String k = "Some\"Key=09";
    private final IMessage.K key = new IMessage.K(k.getBytes());
//    private String value = "==Abc\n09$8";
    private String v = "==Abc09$8";
    private IMessage.V value = new IMessage.V(v.getBytes());

    @Before
    public void init() {
        persistenceManager = new PersistentStorage(dbPath);
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
        String newV = "New\"Key=10";
        IMessage.V newValue = new IMessage.V(newV.getBytes());

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
