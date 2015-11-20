//
// This file is part of the Styx Application.
//
// Styx is a derivative work, containing both original code, included
// code and modified
// code that was published under the GNU General Public License. Copyrights
// for modified and included code are below.
//
// Original code base Copyright 2005 Cai Mao (Swingler). All rights reserved.
//
// Modifications:
//
// 2007 Nov 08 - Removed the syslog object, and replaced with log4j
// 
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
//       
// For more information contact:
// Aaron Paxson <aj@thepaxson5.org>
//

package cai.flow.collector;

/* TODO:  Apparently, the table IpSegments MUST be created first
 * before creating the db.  Why?  Fix this.
 */

import java.io.UnsupportedEncodingException;

import cai.flow.packets.v9.TemplateManager;
import cai.sql.SQL;
import cai.utils.DoneException;
import cai.utils.Params;
import org.apache.log4j.Logger;

public class Run {
    static Logger logger = Logger.getLogger(Run.class.getName());
    
    static {
        try {
            Class.forName("cai.flow.collector.Collector");
        } catch (Exception ex) {
            logger.error("Unable to load Collector - Use debug to see StackTrace");
            logger.debug(ex);
        }
    }

    public static void go(String args[]) {
        
        boolean run_collector = true;
        for (int i = 0; i < args.length; i++) {
            logger.debug("Argument number: " + i);
            logger.debug("Arguments given: " + args.toString());
            if (args[i].equals("create_db")) {
                new SQL().create_DB();
                run_collector = false;
            } else if (args[i].equals("remove_db")) {
                new SQL().delete_DB();
                run_collector = false;
            } else if (args[i].startsWith("encoding=")) {
                Params.encoding = args[i].substring(9);

                try {
                    String test = "eeeeeee";
                    test.getBytes(Params.encoding);
                } catch (UnsupportedEncodingException e) {
                    logger.fatal("Unsupported encoding: " + Params.encoding);
                    System.exit(0);
                }
            } else {
                logger.fatal("Unknown argument -- " + args[i]);
                run_collector = false;
            }
        }
      

        if (run_collector) { 
            TemplateManager.getTemplateManager();
            new Collector().go();
        }
    }

    public static void main(String args[]) throws Throwable {
        try {
            go(args);
        } catch (DoneException e) {
           logger.error("Run error - " + e.toString());
        } catch (Throwable e) {
            logger.error(e.toString());
        }
    }
}
