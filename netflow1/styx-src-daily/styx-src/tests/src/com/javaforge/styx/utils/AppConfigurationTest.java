/*
 * AppConfigurationTest.java
 * JUnit based test
 *
 * Created on December 1, 2007, 9:59 PM
 */

package com.javaforge.styx.utils;

import junit.framework.TestCase;
import org.apache.commons.configuration.Configuration;

/**
 *
 * @author apaxson
 */
public class AppConfigurationTest extends TestCase {
    
    public AppConfigurationTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testGetConfig() {
        System.out.println("getConfig");
        Configuration expResult = null;
        Configuration result = AppConfiguration.getConfig();
        assertEquals(expResult, result);
        fail("The test case is a prototype.");
    } /* Test of getConfig method, of class AppConfiguration. */
    
}
