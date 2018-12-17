package testing;

import protocol.*;
import junit.framework.TestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import client.app.CommandLineApp;
import ecs.client.ECSApplication;

import org.junit.Before;
import org.junit.After;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;


public class ECSAppTest {
	private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);
	
	private ECSApplication testApp;
	private Class[] parameters;
	private Method isValidArgs;
	private Method isValidCacheSize;
	private Method isValidDisplacementStrategy;

	
	@Test
	public void testIsValidCacheSize() throws NoSuchMethodException {
		testApp = new ECSApplication();	
		parameters = new Class[1];
		parameters[0] = String.class;
		isValidCacheSize = testApp.getClass().getDeclaredMethod("isValidCacheSize", parameters);
		isValidCacheSize.setAccessible(true);
		boolean validCacheSize = false;
		boolean invalidCacheSize = true;
		try {
			invalidCacheSize = (boolean) isValidCacheSize.invoke(testApp, "-1073741824");
			validCacheSize = (boolean) isValidCacheSize.invoke(testApp, "600");
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
	    assertTrue(validCacheSize);
	    assertFalse(invalidCacheSize);
	}

	@Test
	public void testisValidDisplacementStrategy() throws NoSuchMethodException {
		testApp = new ECSApplication();	
		parameters = new Class[1];
		parameters[0] = String.class;
		isValidDisplacementStrategy = testApp.getClass().getDeclaredMethod("isValidDisplacementStrategy", parameters);
		isValidDisplacementStrategy.setAccessible(true);
	    boolean validDisplacementStrategy = false;
	    boolean invalidDisplacementStrategy = true;
	    try {
			validDisplacementStrategy = (boolean) isValidDisplacementStrategy.invoke(testApp, "FIFO");
			invalidDisplacementStrategy = (boolean) isValidDisplacementStrategy.invoke(testApp, "invalidstrategy");
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
	    assertTrue(validDisplacementStrategy);
	    assertFalse(invalidDisplacementStrategy);
	}
	
	@Test
	public void testIsValidArgs() throws NoSuchMethodException {
		testApp = new ECSApplication();
		parameters = new Class[2];
		parameters[0] = java.lang.String.class;
		parameters[1] = String[].class;
		isValidArgs = testApp.getClass().getDeclaredMethod("isValidArgs", parameters);
		isValidArgs.setAccessible(true);
		boolean validInit = false;
		boolean invalidInit = true;
		boolean validAdd = false;
		boolean invalidAdd = true;
		String[] invalidInitArgs = {"init", "invalidArgs"};
		String[] validInitArgs = {"init", "5 600 FIFO"};
		String[] invalidAddArgs = {"add", "invalidArgs"};
		String[] validAddArgs = {"add", "600 FIFO"};
		try {
			validInit = (boolean) isValidArgs.invoke(testApp, "init", validInitArgs);
			invalidInit = (boolean) isValidArgs.invoke(testApp, "init", invalidInitArgs);
			validAdd = (boolean) isValidArgs.invoke(testApp, "add", validAddArgs);
			invalidAdd = (boolean) isValidArgs.invoke(testApp, "add", invalidAddArgs);
		} catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
            e.printStackTrace();
        }
		assertTrue(validInit);
		assertFalse(invalidInit);
		assertTrue(validAdd);
		assertFalse(invalidAdd);
	}

}

