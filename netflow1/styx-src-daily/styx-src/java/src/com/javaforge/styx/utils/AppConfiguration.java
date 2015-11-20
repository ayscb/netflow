/*
AppConfiguration.java

Created on Dec 1, 2007, 8:00:32 PM

To change this template, choose Tools | Templates
and open the template in the editor.
 */

package com.javaforge.styx.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

/**
 *
 * @author apaxson
 */
public class AppConfiguration {
    
    private static Configuration config;
    private static Logger logger = Logger.getLogger(AppConfiguration.class.getName());
    
    private AppConfiguration() {
    }
    
    public static Configuration getConfig() {
        if (config == null) {
            try {
                ConfigurationFactory factory = new ConfigurationFactory("../etc/config.xml");
                config = factory.getConfiguration();
            } catch (ConfigurationException ex) {
                logger.fatal("Unable to load the application configuration", ex);
            }
        }
        return config;
    }

}
