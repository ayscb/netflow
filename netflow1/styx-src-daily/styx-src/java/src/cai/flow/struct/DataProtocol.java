//
// This file is part of the Styx Application.
//
// Styx is a derivative work, containing both original code, included code
// and modified code that was published under the GNU General Public License.
// Copyrights for modified and included code are below.
//
// Original code base Copyright 2005 Cai Mao (Swingler). All rights reserved.
//
// Modifications:
//
// 2007-11-14 - Removed Syslog and replaced with Log4J
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
package cai.flow.struct;

import java.io.IOException;
import java.util.LinkedList;
import java.util.ListIterator;

import cai.flow.collector.interpretator.PT_Error;
import cai.flow.collector.interpretator.PT_ExecError;
import cai.flow.collector.interpretator.RuleSet;
import cai.flow.packets.Flow;
import cai.utils.Program;
import org.apache.log4j.Logger;

class Rule {
	public String protocol;

	public RuleSet program;

	public Rule(String protocol, RuleSet program) {
		this.protocol = protocol;
		this.program = program;
	}
}

abstract public class DataProtocol {
        static Logger logger = Logger.getLogger(cai.flow.struct.DataProtocol.class);
        
	static private LinkedList protocol_list = init_protocol_list();

    @SuppressWarnings("unchecked")
	static private LinkedList init_protocol_list() {
		LinkedList ret = new LinkedList();

		try {
			Program program = new Program("Protocols");
			ListIterator e = program.listIterator();

			while (e.hasNext()) {
				String[] obj = (String[]) e.next();

				try {
					RuleSet rule = RuleSet.create(obj[1], obj[0]);

					ret.add(new Rule(obj[0], rule));
				} catch (PT_Error exc) {
					program.error(exc.toString());
				}
			}
		} catch (IOException exc) {
                        logger.error("I/O error while reading Protocols definition",exc);
		}

		return ret;
	}

	public static String aggregate(Flow flow) {
		ListIterator e = protocol_list.listIterator(0);

		while (e.hasNext()) {
			Rule rule = (Rule) e.next();

			try {
				if (rule.program.exec(flow)) {
					/*
					 * System.out.println( "srcpor="+flow.getSrcPort()+ ",
					 * dstport="+flow.getDstPort()+ ", prot="+flow.getProto()+ "
					 * it is ["+rule.protocol+"]" );
					 */
					return rule.protocol;
				}
			} catch (PT_ExecError exc) {
                                logger.error("BUG: aggregate protocol: ",exc);
			}
		}

		return "Other";
	}

}
