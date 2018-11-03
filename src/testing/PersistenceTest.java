//package testing;
//
//import org.junit.Before;
//import org.junit.Test;
//import protocol.K;
//import protocol.V;
//import server.storage.disk.IPersistentStorage.OpStatus;
//import server.storage.disk.PersistentStorage;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.MatcherAssert.assertThat;
//
//public class PersistenceTest {
//    PersistentStorage persistenceManager;
//    private final String k = "Some\"Key=09";
//    private final K key = new K(k.getBytes());
////    private String value = "==Abc\n09$8";
//    private String v = "==Abc09$8";
//    private V value = new V(v.getBytes());
//
//    @Before
//    public void init() {
//        persistenceManager = new PersistentStorage();
//    }
//
//    @Test
//    public void testCRUDFile() {
//        testCreateFile();
//        testReadFile();
//        testUpdateFile();
//        testDeleteFile();
//    }
//
//    private void testDeleteFile() {
//        OpStatus status = persistenceManager.delete(key);
//        assertThat(status, is(OpStatus.DELETE_SUCCESS));
//
//        Path filePath = persistenceManager.getFilePath(key);
//        assertThat(!Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
//    }
//
//    private void testUpdateFile() {
////        String newValue = "New\"Key=10\r";
//        String newV = "New\"Key=10";
//        V newValue = new V(newV.getBytes());
//
//        OpStatus status =  persistenceManager.write(key, newValue);
//        assertThat(status, is(OpStatus.WRITE_SUCCESS));
//
//        String get = persistenceManager.read(key);
//        assertThat(get, equalTo(newValue));
//    }
//
//    private void testReadFile() {
//        String get = persistenceManager.read(key);
//        assertThat(get, equalTo(value));
//    }
//
//    private void testCreateFile() {
//        OpStatus status =  persistenceManager.write(key, value);
//        assertThat(status, is(OpStatus.WRITE_SUCCESS));
//        Path filePath = persistenceManager.getFilePath(key);
//        assertThat(Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
//    }
//}
