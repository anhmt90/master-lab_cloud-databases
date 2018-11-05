package testing;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import protocol.K;
import protocol.V;
import server.storage.PUTStatus;
import server.storage.disk.PersistenceManager;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.CoreMatchers.equalTo;

public class PersistenceTest extends TestCase {
    PersistenceManager persistenceManager;
    private final K key = new K("Some\"Key=\r09".getBytes());
    //    private String value = "==Abc\n09$8";
    private V value = new V("==Abc09$8".getBytes());

    public void init() {
        persistenceManager = new PersistenceManager();
    }

    @Test
    public void testCRUDFile() {
        init();
        testCreateFile();
        testReadFile();
        testUpdateFile();
        testDeleteFile();
    }

    public void testCreateFile() {
        PUTStatus status = persistenceManager.write(key.get(), value.get());
        assertThat(status, is(PUTStatus.CREATE_SUCCESS));
        Path filePath = persistenceManager.getFilePath(key.get());
        assertThat(Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
    }

    public void testReadFile() {
        byte[] get = persistenceManager.read(key.get());
        assertThat(get, equalTo(value.get()));
    }


    public void testUpdateFile() {
//        String newValue = "New\"Key=10\r";
        String newString = "New\"Val=10\r";
        V newValue = new V(newString.getBytes());

        PUTStatus status = persistenceManager.write(key.get(), newValue.get());
        assertThat(status, is(PUTStatus.UPDATE_SUCCESS));

        byte[] get = persistenceManager.read(key.get());
        assertThat(get, equalTo(newValue.get()));
    }

    @Test
    public void testDeleteFile() {
        PUTStatus status = persistenceManager.delete(key.get());
        assertThat(status, is(PUTStatus.DELETE_SUCCESS));

        Path filePath = persistenceManager.getFilePath(key.get());
        assertThat(!Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
    }
}
