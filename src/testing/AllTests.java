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
	public static final String DB_DIR = "test_db";
	private static Logger LOG = LogManager.getLogger(TEST_LOG);
	
    static {
        try {
            new Server("node1",50000, "ERROR");
        } catch (Exception e) {
        	LOG.error(e);
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(CacheTest.class);
        clientSuite.addTestSuite(PersistenceTest.class);
        return clientSuite;
    }

}
