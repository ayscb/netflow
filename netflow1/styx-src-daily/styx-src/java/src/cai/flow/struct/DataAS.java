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
import java.util.Hashtable;
import java.util.ListIterator;
import java.util.StringTokenizer;

import cai.utils.Program;
import org.apache.log4j.Logger;

abstract public class DataAS {
        static Logger logger = Logger.getLogger(cai.flow.struct.DataAS.class);
        
	public static final int AS_Destination = 0;

	public static final int AS_Source = 1;

	public static String aggregate(long as, int table) {
		switch (table) {
		case AS_Source:
			return aggregate_as(src_as_list, as);
		case AS_Destination:
			return aggregate_as(dst_as_list, as);
		}

		throw new IllegalArgumentException("Invalid AS type: " + table);
	}

	static private Hashtable dst_as_list = init_as_list("AS_Destination");

	static private Hashtable src_as_list = init_as_list("AS_Source");

    @SuppressWarnings("unchecked")
	static private Hashtable init_as_list(String name) {
		Hashtable ret = new Hashtable();

		try {
			Program program = new Program(name);//�s�.aggregate�ļ�
			ListIterator e = program.listIterator();

			while (e.hasNext()) {
				String[] obj = (String[]) e.next();
				String as_name = obj[0];

				for (StringTokenizer st = new StringTokenizer(obj[1]); st
						.hasMoreElements();) {
					String as_number = st.nextToken();
					int minus = as_number.indexOf('-');

					if (minus == -1)
						ret.put(as_number, as_name);
					else if (as_number.indexOf('-', minus + 1) != -1)
						program.error("value `" + as_number + "' of `"
								+ as_name + "' is invalid");
					else {
						int start = 0, stop = 0;
						String s_start = as_number.substring(0, minus);
						String s_stop = as_number.substring(minus + 1);

						try {
							start = Integer.parseInt(s_start);
						} catch (NumberFormatException e1) {
							program.error("value `" + s_start + "' of `"
									+ as_name + "' isn't integer");
						}

						try {
							stop = Integer.parseInt(s_stop);
						} catch (NumberFormatException e2) {
							program.error("value `" + s_stop + "' of `"
									+ as_name + "' isn't integer");
						}

						for (int i = start; i <= stop; i++)
							ret.put("" + i, as_name);
					}
				}
			}
		} catch (IOException exc) {
                        logger.error("I/O error while reading " + name
					+ " definition",exc);
		}

		return ret;
	}

	private static String aggregate_as(Hashtable as_list, long as) {
		String ret = (String) as_list.get("" + as);

		return ret == null ? "Other" : ret;
	}

}
