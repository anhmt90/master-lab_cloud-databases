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
    private final String key = "New\"Key=09\n";
    private String value = "==Abc\n@098";

    @Before
    public void init() {
        persistenceManager = new PersistenceManager();
    }

    @Test
    public void testCreateFile() {
        OpStatus status =  persistenceManager.write(key, value);
        assertThat(status, is(OpStatus.WRITE_SUCCESS));

        String quote = String.valueOf((byte)'\"');
        String equal = String.valueOf((byte)'=');
        String zero = String.valueOf((byte)'0');
        String nine = String.valueOf((byte)'9');
        String lineFeed = String.valueOf((byte)'\n');
        String fileName = "New" + quote + "Key" + equal + zero + nine + lineFeed;
        String size = String.valueOf(fileName.length());

        String file = KVServer.ROOT_DB_PATH + "/" + size + "/N/e/w/" + quote + "/K/e/y/"
                + equal + "/" + zero + "/" + nine + "/" + lineFeed
                + "/" + fileName;
        Path filePath = Paths.get(file);
        assertThat(Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
    }
}
