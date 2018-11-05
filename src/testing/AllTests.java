package testing;

import junit.framework.Test;
import junit.framework.TestSuite;
import server.app.Server;
import server.storage.cache.CacheDisplacementStrategy;


public class AllTests {

    static {
        try {
            new Server(50000, 10, CacheDisplacementStrategy.FIFO);
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
