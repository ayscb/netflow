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

import java.util.Enumeration;
import java.util.Vector;

import cai.sql.SQL;
import cai.utils.DoneException;
import cai.utils.Resources;
import cai.utils.Util;

/*

 V7 Flow Packet (Catalyst 5000)

 *-------*---------------*------------------------------------------------------*
 | Bytes | Contents      | Description                                          |
 *-------*---------------*------------------------------------------------------*
 | 0-1   | version       | NetFlow export format version number                 |
 *-------*---------------*------------------------------------------------------*
 | 2-3   | count         | Number of flows exported in this flow frame          |
 |       |               | (protocol data unit, or PDU)                         |
 *-------*---------------*------------------------------------------------------*
 | 4-7   | SysUptime     | Current time in milliseconds since the export device |
 |       |               | booted                                               |
 *-------*---------------*------------------------------------------------------*
 | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
 *-------*---------------*------------------------------------------------------*
 | 12-15 | unix_nsecs    | Residual nanoseconds since 0000 UTC 1970             |
 *-------*---------------*------------------------------------------------------*
 | 16-19 | flow_sequence | Sequence counter of total flows seen                 |
 *-------*---------------*------------------------------------------------------*
 | 20-23 | reserved      | Unused (zero) bytes                                  |
 *-------*---------------*------------------------------------------------------*

 */
import com.javaforge.styx.utils.AppConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

public class V7_Packet implements FlowPacket {
    
        static Configuration config = AppConfiguration.getConfig();
    
        static Logger logger = Logger.getLogger(cai.flow.packets.V7_Packet.class);
    
	long count;

	String RouterIP;

	long SysUptime, unix_secs, unix_nsecs, flow_sequence;

	Vector flows;

	public final int V7_Header_Size = 24;

	public final int V7_Flow_Size = 52;

	static boolean static_initialized = false;

	static boolean replace_source;

    @SuppressWarnings("unchecked")
	public V7_Packet(String RouterIP, Resources resources, byte[] buf, int len)
			throws DoneException {

		if (!static_initialized) {
			replace_source = resources
					.isTrue("flow.collector.replace.V7.source");
			static_initialized = true;
		}

		if (len < V7_Header_Size)
			throw new DoneException("    * incomplete header *");

		this.RouterIP = RouterIP;
		count = Util.to_number(buf, 2, 2);

		if (count <= 0 || len != V7_Header_Size + count * V7_Flow_Size)
			throw new DoneException("    * corrupted packet " + len + "/"
					+ count + "/" + (V7_Header_Size + count * V7_Flow_Size)
					+ " *");

		SysUptime = Util.to_number(buf, 4, 4);
		unix_secs = Util.to_number(buf, 8, 4);
		unix_nsecs = Util.to_number(buf, 12, 4);
		flow_sequence = Util.to_number(buf, 16, 4);

                logger.debug("    uptime: "
					+ Util.uptime(SysUptime / 1000) + ", date: " + unix_secs
					+ "." + unix_nsecs + "    sequence: " + flow_sequence
					+ ", count: " + count);

		flows = new Vector((int) count);

		for (int i = 0, p = V7_Header_Size; i < count; i++, p += V7_Flow_Size)
			flows.add(new V7_Flow(replace_source, RouterIP, buf, p));
	}

	protected static String add_raw_sql = null;

	public void process_raw(SQL sql) {
		if (add_raw_sql == null) {
			add_raw_sql = config.getString("SQL.Add.RawV7");
		}

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			((V7_Flow) flowenum.nextElement()).save_raw(SysUptime, unix_secs,
					unix_nsecs, flow_sequence, sql.prepareStatement(
							"Prepare INSERT to V7 raw table", add_raw_sql));
	}

    @SuppressWarnings("unchecked")
	public Vector getSrcASVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataSrcAS());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getDstASVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataDstAS());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getASMatrixVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataASMatrix());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getSrcNodeVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataSrcNode());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getDstNodeVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataDstNode());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getHostMatrixVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataHostMatrix());

		return v;
	}

	public Vector getSrcInterfaceVector() {
		return null;
	}

    @SuppressWarnings("unchecked")
	public Vector getDstInterfaceVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataDstInterface());

		return v;
	}

    @SuppressWarnings("unchecked")
	public Vector getInterfaceMatrixVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataInterfaceMatrix());

		return v;
	}

	public Vector getSrcPrefixVector() {
		return null;
	}

	public Vector getDstPrefixVector() {
		return null;
	}

	public Vector getPrefixMatrixVector() {
		return null;
	}

    @SuppressWarnings("unchecked")
	public Vector getProtocolVector() {
		Vector v = new Vector((int) count, (int) count);

		for (Enumeration flowenum = flows.elements(); flowenum
				.hasMoreElements();)
			v.add(((V7_Flow) flowenum.nextElement()).getDataProtocol());

		return v;
	}
}
