package testing;

import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import client.api.Client;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.junit.AfterClass;
import server.app.Server;
import server.storage.cache.CacheDisplacementStrategy;
import util.HashUtils;
import util.Validate;


public class AllTests {
    public static final String TEST_LOG = "tests";
    public static final String DB_DIR = "test_db";
    private static Logger LOG = LogManager.getLogger(TEST_LOG);
    static Server server = null;

    static {
        try {
            server = new Server("node1", 50000, 60000, "ERROR");
            server.start();
            server.initKVServer(getMetadata(), 100, "FIFO");
            server.startService();
//            Thread.sleep(3600000);
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(CacheTest.class);
        clientSuite.addTestSuite(ClientAppTest.class);
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(ECSAppTest.class);
        clientSuite.addTestSuite(FetchBatchDataTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(KeyRangeTest.class);
        clientSuite.addTestSuite(MarshallingTest.class);
        clientSuite.addTestSuite(PersistenceTest.class);
        return clientSuite;
    }

    private static Metadata getMetadata() {
        String node1Hash = new String(new char[8]).replace("\0", "3333");
        String node2Hash = new String(new char[8]).replace("\0", "9999");
        String node3Hash = new String(new char[8]).replace("\0", "eeee");

        KeyHashRange node1_range = new KeyHashRange(HashUtils.increaseHashBy1(node3Hash), node1Hash); // EEEE..EEEF - 3333..3333
        KeyHashRange node2_range = new KeyHashRange(HashUtils.increaseHashBy1(node1Hash), node2Hash); // 3333..3334 - 9999..9999
        KeyHashRange node3_range = new KeyHashRange(HashUtils.increaseHashBy1(node2Hash), node3Hash); // 9999..999A - EEEE..EEEE

        Metadata metadata = new Metadata();
        NodeInfo nodeInfo1 = new NodeInfo("node1", "127.0.0.1", 50000, node1_range);
        NodeInfo nodeInfo2 = new NodeInfo("node2", "127.0.0.1", 50001, node2_range);
        NodeInfo nodeInfo3 = new NodeInfo("node3", "127.0.0.1", 50002, node3_range);
        metadata.add(nodeInfo1);
        metadata.add(nodeInfo2);
        metadata.add(nodeInfo3);

        return metadata;
    }

    @AfterClass
    public static void doYourOneTimeTeardown() {
        Validate.notNull(server, "Server has not been initialized yet!");
        server.shutdown();
    }

}
