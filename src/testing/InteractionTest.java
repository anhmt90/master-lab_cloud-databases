package testing;

import client.api.Client;
import junit.framework.TestCase;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import protocol.IMessage;
import protocol.IMessage.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class InteractionTest {
    private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

    private Client kvClient;

    @Before
    public void setUp() {
        kvClient = new Client("127.0.0.1", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    @After
    public void tearDown() {
        if (kvClient != null)
            kvClient.disconnect();
    }


    @Test
    public void testPut() throws IOException {
        String key = "foo";
        String value = "bar";
        IMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            LOG.error(e);
            ex = e;
            throw e;
        }

        assertTrue(ex == null && response.getStatus() == Status.PUT_SUCCESS || response.getStatus() == Status.PUT_UPDATE);
    }

    @Test
    public void testPutDisconnected() {
        kvClient.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            LOG.error(e);
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        IMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);

        } catch (Exception e) {
            LOG.error(e);
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        IMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, "null");

        } catch (Exception e) {
            LOG.error(e);
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        IMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            LOG.error(e);
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        IMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            LOG.error(e);
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.GET_ERROR);
    }

}