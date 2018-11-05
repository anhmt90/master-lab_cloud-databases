package testing;

import client.api.Client;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import protocol.IMessage;
import protocol.IMessage.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class InteractionTest extends TestCase {

    private Client kvClient;

    @Before
    public void setUp() {
        kvClient = new Client("localhost", 50000);
        try {
            kvClient.connect();
            byte[] bytes = kvClient.receive();
            String welcomeMessage = new String(bytes, StandardCharsets.US_ASCII).trim();
            System.out.println(welcomeMessage);
        } catch (Exception e) {
        }
    }

    @After
    public void tearDown() {
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
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.PUT_UPDATE
                && new String(response.getValue().get()).equals(updatedValue));
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
            ex = e;
        }

        assertTrue(ex == null && new String(response.getValue().get()).equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        IMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.GET_ERROR);
    }

}