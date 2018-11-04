package testing;

import client.api.Client;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import server.app.StorageServer;
import server.storage.Cache.CacheDisplacementType;

import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {

    @Test
    public void testConnectionSuccess() {
        Exception ex = null;

        Client kvClient = new Client("127.0.0.1", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
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
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

}

