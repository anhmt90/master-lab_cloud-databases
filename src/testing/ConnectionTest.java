package testing;

import client.api.Client;
import junit.framework.TestCase;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ConnectionTest {
	private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);
	
    @Test
    public void testConnectionSuccess() {
        Exception ex = null;

        Client kvClient = new Client("127.0.0.1", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        	LOG.error(e);
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testUnknownHost() {
        Exception ex = null;
        Client kvClient = new Client("unknown", 50000);

        try {
            kvClient.connect();
        } catch (Exception e) {
        	LOG.error(e);
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }


    @Test
    public void testIllegalPort() {
        Exception ex = null;
        Client kvClient = new Client("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
        	LOG.error(e);
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

}

