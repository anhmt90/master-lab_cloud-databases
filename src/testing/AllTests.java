package testing;

import junit.framework.Test;
import junit.framework.TestSuite;
import server.app.StorageServer;
import server.storage.Cache.CacheDisplacementType;


public class AllTests {

    static {
        try {
            new StorageServer(50000, 10, CacheDisplacementType.FIFO);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(AdditionalTest.class);
        return clientSuite;
    }

}
