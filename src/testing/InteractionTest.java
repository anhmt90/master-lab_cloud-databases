package testing;

import client.api.Client;
import protocol.IMessage;
import protocol.IMessage.Status;
import junit.framework.TestCase;
import org.junit.Test;


public class InteractionTest extends TestCase {

    private Client kvClient;

    public void setUp() {
        kvClient = new Client("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }


    @Test
    public void testPut() {
        String key = "foo";
        String value = "bar";
        IMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.PUT_SUCCESS);
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
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == Status.GET_ERROR);
    }

}