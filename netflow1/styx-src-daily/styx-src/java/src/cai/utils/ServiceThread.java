//
// This file is part of the Styx Application.
//
// Styx is a derivative work, containing both original code, included
// code and modified
// code that was published under the GNU General Public License. Copyrights
// for modified and included code are below.
//
// Original code base Copyright 2000-2001 Cai Mao & cai. All rights reserved.
//
// Modifications:
//
// 2007 Nov 09 - Added log4j logging instead of syslog
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


package cai.utils;

import org.apache.log4j.Logger;

public abstract class ServiceThread extends Thread {
    
        static Logger logger = Logger.getLogger(ServiceThread.class.getName());
        
	protected Object o;

	String start, stop;

	Syslog syslog;

	public ServiceThread(Object o, Syslog syslog, String start, String stop) {
		this.o = o;
		this.syslog = syslog;
		this.start = start;
		this.stop = stop;
	}
        
        public ServiceThread(Object o, String start, String stop) {
            this.o = o;
            this.start=start;
            this.stop=stop;
        }

	public void run() {
            if (syslog!=null) {
		syslog.syslog(Syslog.LOG_NOTICE, "START: " + getName() + ", " + start);
            } else {
                logger.info("START: " + getName() + ", " + start);
            }
		try {
			exec();
		} catch (Throwable e) {
                        logger.error(e + ". Use debug to see stack trace");
                        logger.debug("Exception: ",e);
		}
                if (syslog!=null) {
                    syslog.syslog(Syslog.LOG_NOTICE, "STOP: " + getName() + ", " + stop);
                } else {
                    logger.info("STOP: " + getName() + ", " + stop);
                }
	}

	public abstract void exec() throws Throwable;
}
