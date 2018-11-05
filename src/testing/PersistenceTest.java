//package testing;
//
//import org.junit.Before;
//import org.junit.Test;
//import protocol.K;
//import protocol.V;
//import server.storage.disk.IPersistenceManager.PUTStatus;
//import server.storage.disk.PersistenceManager;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.MatcherAssert.assertThat;
//
//public class PersistenceTest {
//    PersistenceManager persistenceManager;
//    private final String k = "Some\"Key=09";
//    private final K key = new K(k.getBytes());
////    private String value = "==Abc\n09$8";
//    private String v = "==Abc09$8";
//    private V value = new V(v.getBytes());
//
//    @Before
//    public void init() {
//        persistenceManager = new PersistenceManager();
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
//        PUTStatus status = persistenceManager.delete(key);
//        assertThat(status, is(PUTStatus.DELETE_SUCCESS));
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
//        PUTStatus status =  persistenceManager.write(key, newValue);
//        assertThat(status, is(PUTStatus.WRITE_SUCCESS));
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
//        PUTStatus status =  persistenceManager.write(key, value);
//        assertThat(status, is(PUTStatus.WRITE_SUCCESS));
//        Path filePath = persistenceManager.getFilePath(key);
//        assertThat(Files.exists(filePath) && !Files.isDirectory(filePath), equalTo(Boolean.TRUE));
//    }
//}
