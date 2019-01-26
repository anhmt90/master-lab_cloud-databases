package testing;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import protocol.kv.K;
import protocol.kv.V;
import server.storage.PUTStatus;
import server.storage.disk.PersistenceManager;
import util.FileUtils;
import util.HashUtils;

import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.CoreMatchers.equalTo;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PersistenceTest {
    PersistenceManager persistenceManager = new PersistenceManager(AllTests.DB_DIR);
    private final K key = new K(HashUtils.digest("SomeKey=09"));
    private V value = new V(getMaxLengthString().getBytes());


    @Test
    public void test1CreateFile() {
        PUTStatus status = persistenceManager.write(key.getHashed(), value.get());
        assertThat(status, is(PUTStatus.CREATE_SUCCESS));
        Path filePath = persistenceManager.getFilePath(key.getHashed());
        assertThat(FileUtils.exists(filePath) && !FileUtils.isDir(filePath), equalTo(Boolean.TRUE));
    }

    @Test
    public void test2ReadFile() {
        byte[] get = persistenceManager.read(key.getHashed());
        assertThat(get, equalTo(value.get()));
    }

    @Test
    public void test3UpdateFile() {
        //Update
        String newString = "New\"Val=10";
        V newValue = new V(newString.getBytes());
        PUTStatus status = persistenceManager.write(key.getHashed(), newValue.get());
        assertThat(status, is(PUTStatus.UPDATE_SUCCESS));

        //Read
        byte[] get = persistenceManager.read(key.getHashed());
        assertThat(get, equalTo(newValue.get()));

    }

    @Test
    public void test4DeleteFile() {
        PUTStatus status = persistenceManager.delete(key.getHashed());
        assertThat(status, is(PUTStatus.DELETE_SUCCESS));

        Path filePath = persistenceManager.getFilePath(key.getHashed());
        assertThat(!FileUtils.exists(filePath) && !FileUtils.isDir(filePath), equalTo(Boolean.TRUE));
    }

    private String getMaxLengthString() {
        return new String(new char[1024*24]).replace("\0", "ab cd");
    }

}
