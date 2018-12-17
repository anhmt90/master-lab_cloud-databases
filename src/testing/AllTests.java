package testing;

import ecs.ExternalConfigurationService;
import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import client.api.Client;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import server.app.Server;
import server.storage.cache.CacheDisplacementStrategy;
import util.HashUtils;
import util.Validate;

import java.io.IOException;

import static util.FileUtils.SEP;
import static util.FileUtils.USER_DIR;



@RunWith(Suite.class)
@Suite.SuiteClasses({
        CacheTest.class,
        ClientAppTest.class,
        ConnectionTest.class,
        ECSAppTest.class,
        FetchBatchDataTest.class,
        InteractionTest.class,
        KeyRangeTest.class,
        MarshallingTest.class,
        PersistenceTest.class})
public class AllTests {
    public static final String TEST_LOG = "tests";
    public static final String DB_DIR = "test_db";
    private static Logger LOG = LogManager.getLogger(TEST_LOG);
    private static final String ECS_CONFIG_PATH = USER_DIR + SEP + "config" + SEP + "test-server-info";
    static ExternalConfigurationService ecs = null;

    @BeforeClass
    public static void startECS() {
        try {
            ecs = new ExternalConfigurationService(ECS_CONFIG_PATH);
            ecs.initService(1, 100, "FIFO");
            ecs.startService();
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void shutdownService() throws IOException, InterruptedException {
        Validate.notNull(ecs, "ECS has not been initialized yet!");
        ecs.shutdown();
        ecs.getReportManager().getReportSocket().close();
        ecs = null;
    }

}
