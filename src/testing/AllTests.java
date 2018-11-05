package testing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import client.api.Client;
import junit.framework.Test;
import junit.framework.TestSuite;
import server.app.Server;
import server.storage.cache.CacheDisplacementStrategy;


public class AllTests {
	public static final String TEST_LOG = "tests";
	private static Logger LOG = LogManager.getLogger(TEST_LOG);
	
    static {
        try {
            new Server(50000, 10, CacheDisplacementStrategy.FIFO, "ERROR");
        } catch (Exception e) {
        	LOG.error(e);
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
