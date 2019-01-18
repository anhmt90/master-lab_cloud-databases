package testing;

import client.app.CommandLineApp;
import protocol.kv.*;
import protocol.kv.IMessage;
import protocol.kv.IMessage.Status;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ClientAppTest {
	private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);
	
	
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;

	private CommandLineApp testApp;
	private Class[] parameters;
	private Method handleServerResponse;
	private Method isClientConnected;
	private Method isValidPortNumber;
	private Method isValidArgs;
	
	
	@Test
	public void testIsClientConnected() throws NoSuchMethodException {
		testApp = new CommandLineApp();		
		isClientConnected = testApp.getClass().getDeclaredMethod("isClientConnected");
		isClientConnected.setAccessible(true);
		boolean connected = true;
		try {
			connected = (boolean) isClientConnected.invoke(testApp);
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
	    assertFalse(connected);
	}

	@Test
	public void testIsValidPortNumber() throws NoSuchMethodException {
		testApp = new CommandLineApp();	
		parameters = new Class[1];
		parameters[0] = String.class;
		isValidPortNumber = testApp.getClass().getDeclaredMethod("isValidPortNumber", parameters);
		isValidPortNumber.setAccessible(true);
	    boolean invalidPort = true;
	    boolean validPort = false;
	    try {
			invalidPort = (boolean) isValidPortNumber.invoke(testApp, "777777");
			validPort = (boolean) isValidPortNumber.invoke(testApp, "50000");
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
	    assertFalse(invalidPort);
	    assertTrue(validPort);
	}
	
	@Test
	public void testIsValidArgs() throws NoSuchMethodException {
		testApp = new CommandLineApp();
		parameters = new Class[2];
		parameters[0] = java.lang.String.class;
		parameters[1] = String[].class;
		isValidArgs = testApp.getClass().getDeclaredMethod("isValidArgs", parameters);
		isValidArgs.setAccessible(true);
		boolean validGet = true;
		String[] invalidGetArgs = {"get", "get withwhitespace"};
		try {
			validGet = (boolean) isValidArgs.invoke(testApp, "get", invalidGetArgs);
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
		assertFalse(validGet);
	}
	
	@Test
    public void testResponseHandling() throws NoSuchMethodException {
		testApp = new CommandLineApp();	
		parameters = new Class[2];
		parameters[0] = IMessage.class;
		parameters[1] = java.lang.String.class;
		handleServerResponse = testApp.getClass().getDeclaredMethod("handleServerResponse", parameters);
		handleServerResponse.setAccessible(true);
		IMessage serverResponse1 = new Message(Status.GET_SUCCESS, new K("key".getBytes(StandardCharsets.US_ASCII)),new V("value".getBytes(StandardCharsets.US_ASCII)));
		System.setOut(new PrintStream(outContent));
		try {
			handleServerResponse.invoke(testApp, serverResponse1, "key");
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
		assertEquals("Value stored on server for key '" + "key" + "' is: " + "value", outContent.toString());
		outContent.reset();
	    System.setOut(originalOut);
    }

}

