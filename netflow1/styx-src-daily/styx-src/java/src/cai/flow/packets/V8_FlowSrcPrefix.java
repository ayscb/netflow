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
// 2007-12-02 - No longer using ResourceBundles for configuration.
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
package cai.flow.packets;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import cai.flow.struct.Prefix;
import cai.flow.struct.Scheme_DataASMatrix;
import cai.flow.struct.Scheme_DataDstAS;
import cai.flow.struct.Scheme_DataInterface;
import cai.flow.struct.Scheme_DataInterfaceMatrix;
import cai.flow.struct.Scheme_DataPrefix;
import cai.flow.struct.Scheme_DataPrefixMatrix;
import cai.flow.struct.Scheme_DataProtocol;
import cai.flow.struct.Scheme_DataSrcAS;
import cai.sql.SQL;
import cai.utils.DoneException;
import cai.utils.Params;
import cai.utils.Util;
import com.javaforge.styx.utils.AppConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/*

 V8 Flow SrcPrefix structure

 +-------------------------------------------------------------------------------------+
 | Bytes | Contents   | Description                                                    |
 |-------+------------+----------------------------------------------------------------|
 | 0-3   | flows      | Number of flows                                                |
 |-------+------------+----------------------------------------------------------------|
 | 4-7   | dPkts      | Packets in the flow                                            |
 |-------+------------+----------------------------------------------------------------|
 | 8-11  | dOctets    | Total number of Layer 3 bytes in the packets of the flow       |
 |-------+------------+----------------------------------------------------------------|
 | 12-15 | First      | SysUptime, in seconds, at start of flow                        |
 |-------+------------+----------------------------------------------------------------|
 | 16-19 | Last       | SysUptime, in seconds, at the time the last packet of the flow |
 |       |            | was received                                                   |
 |-------+------------+----------------------------------------------------------------|
 | 20-23 | src_prefix | Source IP address prefix                                       |
 |-------+------------+----------------------------------------------------------------|
 | 24    | src_mask   | Source address prefix mask                                     |
 |-------+------------+----------------------------------------------------------------|
 | 25    | pad        | Unused (zero) bytes                                            |
 |-------+------------+----------------------------------------------------------------|
 | 26-27 | src_as     | Source autonomous system number, either origin or peer         |
 |-------+------------+----------------------------------------------------------------|
 | 28-29 | input      | Interface index (ifindex) of input interface                   |
 |-------+------------+----------------------------------------------------------------|
 | 30-31 | reserved   | Unused (zero) bytes                                            |
 +-------------------------------------------------------------------------------------+

 */

public class V8_FlowSrcPrefix extends V8_Flow {
    
        static Configuration config = AppConfiguration.getConfig();
    
        static Logger logger = Logger.getLogger(cai.flow.packets.V8_FlowSrcPrefix.class);
        
	long src_prefix;

	byte src_mask;

	long src_as, input;

	Prefix srcprefix;

	public V8_FlowSrcPrefix(String RouterIP, byte[] buf, int off)
			throws DoneException {
		super(RouterIP, buf, off);

		src_prefix = Util.to_number(buf, off + 20, 4);
		src_mask = buf[off + 24];
		src_as = Util.to_number(buf, off + 26, 2);
		input = Util.to_number(buf, off + 28, 2);

		srcprefix = new Prefix(src_prefix, src_mask);

                logger.debug("      <- AS " + src_as + " "
					+ srcprefix.toString() + ", " + flows + " flows");
                logger.debug("        bytes=" + dOctets
					+ ", pkts=" + dPkts + ", inIf=" + input);
	}

	public Long getSrcAS() {
		return new Long(src_as);
	}

	public Long getInIf() {
		return new Long(input);
	}

	public Prefix getSrcPrefix() {
		return srcprefix;
	}

	void fill_specific(PreparedStatement add_raw_stm) throws SQLException {
		add_raw_stm.setString(13, Util.str_addr(src_prefix));
		add_raw_stm.setInt(14, (int) src_mask);
		add_raw_stm.setInt(15, (int) src_as);
		add_raw_stm.setInt(16, (int) input);
		add_raw_stm.setString(17, Params.getCurrentTime());
	}

	String table_name() {
		return new String("SrcPrefix");
	}

	protected static String add_raw_sql = null;

	String get_text_raw_insert(SQL sql) {
		return add_raw_sql == null ? config.getString("SQL.Add.RawV8.SrcPrefix") : add_raw_sql;
	}

	PreparedStatement get_sql_raw_insert(SQL sql) throws SQLException {
		return sql.prepareStatement("Prepare INSERT to V8 SrcPrefix raw table",
				get_text_raw_insert(sql));
	}

	public Scheme_DataSrcAS getDataSrcAS() {
		return new Scheme_DataSrcAS(RouterIP, flows, 0, // ???
				dPkts, dOctets, src_as);
	}

	public Scheme_DataDstAS getDataDstAS() {
		return null;
	}

	public Scheme_DataASMatrix getDataASMatrix() {
		return null;
	}

	public Scheme_DataInterface getDataSrcInterface() {
		return new Scheme_DataInterface(RouterIP, flows, 0, // ???
				dPkts, dOctets, input);
	}

	public Scheme_DataInterface getDataDstInterface() {
		return null;
	}

	public Scheme_DataInterfaceMatrix getDataInterfaceMatrix() {
		return null;
	}

	public Scheme_DataPrefix getDataSrcPrefix() {
		return new Scheme_DataPrefix(RouterIP, flows, 0, // ???
				dPkts, dOctets, srcprefix, src_as, input);
	}

	public Scheme_DataPrefix getDataDstPrefix() {
		return null;
	}

	public Scheme_DataPrefixMatrix getDataPrefixMatrix() {
		return null;
	}

	public Scheme_DataProtocol getDataProtocol() {
		return null;
	}
}
