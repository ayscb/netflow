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
// 2007-12-02 - No longer using ResourceBundles for configuration
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

 V8 Flow AS structure

 +-------------------------------------------------------------------------------------+
 | Bytes | Contents | Description                                                      |
 |-------+----------+------------------------------------------------------------------|
 | 0-3   | flows    | Number of flows                                                  |
 |-------+----------+------------------------------------------------------------------|
 | 4-7   | dPkts    | Packets in the flow                                              |
 |-------+----------+------------------------------------------------------------------|
 | 8-11  | dOctets  | Total number of Layer 3 bytes in the packets of the flow         |
 |-------+----------+------------------------------------------------------------------|
 | 12-15 | First    | SysUptime, in seconds, at start of flow                          |
 |-------+----------+------------------------------------------------------------------|
 | 16-19 | Last     | SysUptime, in seconds, at the time the last packet of the flow   |
 |       |          | was received                                                     |
 |-------+----------+------------------------------------------------------------------|
 | 20-21 | src_as   | Source autonomous system number, either origin or peer           |
 |-------+----------+------------------------------------------------------------------|
 | 22-23 | dst_as   | Destination autonomous system number, either origin or peer      |
 |-------+----------+------------------------------------------------------------------|
 | 24-25 | input    | Interface index (ifindex) of input interface                     |
 |-------+----------+------------------------------------------------------------------|
 | 26-27 | output   | Interface index (ifindex) of output interface                    |
 +-------------------------------------------------------------------------------------+

 */

public class V8_FlowAS extends V8_Flow {
    
        static Configuration config = AppConfiguration.getConfig();
    
        static Logger logger = Logger.getLogger(cai.flow.packets.V8_FlowAS.class);
        
	long src_as, dst_as;

	long input, output;

	public V8_FlowAS(String RouterIP, byte[] buf, int off) throws DoneException {
		super(RouterIP, buf, off);

		src_as = Util.to_number(buf, off + 20, 2);
		dst_as = Util.to_number(buf, off + 22, 2);
		input = Util.to_number(buf, off + 24, 2);
		output = Util.to_number(buf, off + 26, 2);

                logger.debug("      AS " + src_as + " -> AS "
					+ dst_as + ", " + flows + " flows");
                logger.debug("        bytes=" + dOctets
					+ ", pkts=" + dPkts + ", inIf=" + input + ", outIf="
					+ output);
	}

	void fill_specific(PreparedStatement add_raw_stm) throws SQLException {
		add_raw_stm.setInt(13, (int) src_as);
		add_raw_stm.setInt(14, (int) dst_as);
		add_raw_stm.setInt(15, (int) input);
		add_raw_stm.setInt(16, (int) output);
		add_raw_stm.setString(17, Params.getCurrentTime());
	}

	String table_name() {
		return new String("AS");
	}

	protected static String add_raw_sql = null;

	String get_text_raw_insert(SQL sql) {
		return add_raw_sql == null ? config.getString("SQL.Add.RawV8.AS") : add_raw_sql;
	}

	PreparedStatement get_sql_raw_insert(SQL sql) throws SQLException {
		return sql.prepareStatement("Prepare INSERT to V8 AS raw table",
				get_text_raw_insert(sql));
	}

	public Scheme_DataSrcAS getDataSrcAS() {
		return null;
	}

	public Scheme_DataDstAS getDataDstAS() {
		return null;
	}

	public Scheme_DataASMatrix getDataASMatrix() {
		return new Scheme_DataASMatrix(RouterIP, flows, 0, // ???
				dPkts, dOctets, src_as, dst_as);
	}

	public Scheme_DataInterface getDataSrcInterface() {
		return null;
	}

	public Scheme_DataInterface getDataDstInterface() {
		return null;
	}

	public Scheme_DataInterfaceMatrix getDataInterfaceMatrix() {
		return null;
	}

	public Scheme_DataPrefix getDataSrcPrefix() {
		return null;
	}

	public Scheme_DataPrefix getDataDstPrefix() {
		return null;
	}

	public Scheme_DataPrefixMatrix getDataPrefixMatrix() {
		return null;
	}

	public Scheme_DataProtocol getDataProtocol() {
		return new Scheme_DataProtocol(RouterIP, 1, 0, dPkts, dOctets, this);
	}
}
