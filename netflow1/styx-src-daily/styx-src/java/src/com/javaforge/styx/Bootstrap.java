/*
Bootstrap.java

Created on Nov 24, 2007, 8:06:51 PM

To change this template, choose Tools | Templates
and open the template in the editor.
 */

package com.javaforge.styx;

import cai.flow.collector.Run;
import org.apache.log4j.Logger;

/**
 *
 * @author apaxson
 */
public class Bootstrap {

    /**
     * @param args the command line arguments
     */
    
    static Logger logger = Logger.getLogger(Bootstrap.class.getName());
    
    public static void shutdown() {
        logger.info("Styx server shutting down....");
        // Shutdown code here
    }
    
    public static void main(String[] args) {
        
        // Register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
        
        // Startup the collector
        logger.info("Styx server is starting up...");
        Run.go(args);
    }

}
