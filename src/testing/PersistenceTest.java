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
    private final K key = new K("SomeKey=09");
    private V value = new V(getMaxLengthString());
    Path filePath = FileUtils.buildPath(persistenceManager.getDbPath(), key.getHashed(),key.getByteString());


    @Test
    public void test1CreateFile() {
        PUTStatus status = persistenceManager.write(filePath, value.getBytes());
        assertThat(status, is(PUTStatus.CREATE_SUCCESS));
        assertThat(FileUtils.exists(filePath) && !FileUtils.isDir(filePath), equalTo(Boolean.TRUE));
    }

    @Test
    public void test2ReadFile() {
        String got = new String (persistenceManager.read(filePath));
        assertThat(got, equalTo(value.get()));
    }

    @Test
    public void test3UpdateFile() {
        //Update
        String newString = "New\"Val=10";
        V newValue = new V(newString);
        PUTStatus status = persistenceManager.write(filePath, newValue.getBytes());
        assertThat(status, is(PUTStatus.UPDATE_SUCCESS));

        //Read
        String got = new String(persistenceManager.read(filePath));
        assertThat(got, equalTo(newValue.get()));

    }

    @Test
    public void test4DeleteFile() {
        PUTStatus status = persistenceManager.delete(filePath);
        assertThat(status, is(PUTStatus.DELETE_SUCCESS));

        assertThat(!FileUtils.exists(filePath) && !FileUtils.isDir(filePath), equalTo(Boolean.TRUE));
    }

    private String getMaxLengthString() {
        return new String(new char[1024 * 24]).replace("\0", "ab cd");
    }

}
