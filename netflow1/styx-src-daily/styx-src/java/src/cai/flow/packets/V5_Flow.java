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
package cai.flow.packets;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import cai.flow.packets.v9.FieldDefinition;
import cai.flow.packets.v9.Template;
import cai.flow.struct.Address;
import cai.flow.struct.Prefix;
import cai.flow.struct.Scheme_DataASMatrix;
import cai.flow.struct.Scheme_DataDstAS;
import cai.flow.struct.Scheme_DataHostMatrix;
import cai.flow.struct.Scheme_DataInterface;
import cai.flow.struct.Scheme_DataInterfaceMatrix;
import cai.flow.struct.Scheme_DataNode;
import cai.flow.struct.Scheme_DataPrefix;
import cai.flow.struct.Scheme_DataPrefixMatrix;
import cai.flow.struct.Scheme_DataProtocol;
import cai.flow.struct.Scheme_DataSrcAS;
import cai.sql.SQL;
import cai.utils.*;

/*
 V5 Flow structure��һ����Ľṹ
 V9��data flowset��¼Ҳ�����ṹ���

 *-------*-----------*----------------------------------------------------------*
 | Bytes | Contents  | Description                                              |
 *-------*-----------*----------------------------------------------------------*
 | 0-3   | srcaddr   | Source IP address                                        |
 *-------*-----------*----------------------------------------------------------*
 | 4-7   | dstaddr   | Destination IP address                                   |
 *-------*-----------*----------------------------------------------------------*
 | 8-11  | nexthop   | IP address of next hop router                            |
 *-------*-----------*----------------------------------------------------------*
 | 12-13 | input     | Interface index (ifindex) of input interface             |
 *-------*-----------*----------------------------------------------------------*
 | 14-15 | output    | Interface index (ifindex) of output interface            |
 *-------*-----------*----------------------------------------------------------*
 | 16-19 | dPkts     | Packets in the flow                                      |
 *-------*-----------*----------------------------------------------------------*
 | 20-23 | dOctets   | Total number of Layer 3 bytes in the packets of the flow |
 *-------*-----------*----------------------------------------------------------*
 | 24-27 | First     | SysUptime at start of flow                               |
 *-------*-----------*----------------------------------------------------------*
 | 28-31 | Last      | SysUptime at the time the last packet of the flow was    |
 |       |           | received                                                 |
 *-------*-----------*----------------------------------------------------------*
 | 32-33 | srcport   | TCP/UDP source port number or equivalent                 |
 *-------*-----------*----------------------------------------------------------*
 | 34-35 | dstport   | TCP/UDP destination port number or equivalent            |
 *-------*-----------*----------------------------------------------------------*
 | 36    | pad1      | Unused (zero) bytes                                      |
 *-------*-----------*----------------------------------------------------------*
 | 37    | tcp_flags | Cumulative OR of TCP flags                               |
 *-------*-----------*----------------------------------------------------------*
 | 38    | prot      | IP protocol type (for example, TCP = 6; UDP = 17)        |
 *-------*-----------*----------------------------------------------------------*
 | 39    | tos       | IP type of service (ToS)                                 |
 *-------*-----------*----------------------------------------------------------*
 | 40-41 | src_as    | Autonomous system number of the source, either origin or |
 |       |           | peer                                                     |
 *-------*-----------*----------------------------------------------------------*
 | 42-43 | dst_as    | Autonomous system number of the destination, either      |
 |       |           | origin or peer                                           |
 *-------*-----------*----------------------------------------------------------*
 | 44    | src_mask  | Source address prefix mask bits                          |
 *-------*-----------*----------------------------------------------------------*
 | 45    | dst_mask  | Destination address prefix mask bits                     |
 *-------*-----------*----------------------------------------------------------*
 | 46-47 | pad2      | Unused (zero) bytes                                      |
 *-------*-----------*----------------------------------------------------------*

 */
import org.apache.log4j.Logger;

public class V5_Flow extends Flow {
    
    static Logger logger = Logger.getLogger(cai.flow.packets.V5_Flow.class);
    
    String srcaddr = "", dstaddr = "", nexthop = "";

    Prefix srcprefix, dstprefix;

    long input = -1, output = -1;

    long dPkts = 0, dOctets = 0, First = 0, Last = 0;

    long srcport = -1, dstport = -1;

    byte tcp_flags = 0, prot = -1, tos = 0;

    long src_as = -1, dst_as = -1;

    byte src_mask = 0, dst_mask = 0;

    String RouterIP = "";

    long src_addr = 0, dst_addr = 0, next_hop = 0;

    public V5_Flow(String RouterIP, final byte[] buf, int off, Template t) throws
            DoneException {
        this.RouterIP = RouterIP;
        if (buf.length < t.getTypeOffset( -1)) { // �����жϣ�ȥ��,���
            throw new DoneException("��Ȳ����template" + t.getTemplateId() + "Ҫ��");
        }
        // ����offset�ͳ���Ϊ�Ƿ�ֵ�����������
        int currOffset = 0, currLen = 0;
        currOffset = t.getTypeOffset(FieldDefinition.IPV4_SRC_ADDR);
        currLen = t.getTypeLen(FieldDefinition.IPV4_SRC_ADDR);
        if (currOffset >= 0 && currLen > 0) {
            srcaddr = Util.str_addr(src_addr = Util.to_number(buf, off
                    + currOffset, currLen));
            if (Params.isSrcExcludes(src_addr)) {
                throw new DoneException(""); //���������
            }
            if (!Params.isSrcIncludes(src_addr)) {
                throw new DoneException("");
            }
            if (srcaddr.startsWith("0.")) {
                System.err.println("ERROR:Template is " + t.getTemplateId() +
                                   " router is " + t.getRouterIp() +
                                   " has srcaddr like 0.");
                throw new DoneException("savePacketF_"+RouterIP+"_"+t.getTemplateId());
            }
        }
        currOffset = t.getTypeOffset(FieldDefinition.IPV4_DST_ADDR);
        currLen = t.getTypeLen(FieldDefinition.IPV4_DST_ADDR);
        if (currOffset >= 0 && currLen > 0) {
            dstaddr = Util.str_addr(dst_addr = Util.to_number(buf, off
                    + currOffset, currLen));
            if (Params.isDstExcludes(dst_addr)) {
                throw new DoneException("");
            }
            if (!Params.isDstIncludes(dst_addr)) {
                throw new DoneException("savePacketF_"+RouterIP+"_"+t.getTemplateId());
            }
            if (srcaddr.startsWith("0.")) {
                System.err.println("ERROR:Template is " + t.getTemplateId() +
                                   " router is " + t.getRouterIp() +
                                   " has srcaddr like 0.");
                throw new DoneException("");
            }
        }
        currOffset = t.getTypeOffset(FieldDefinition.IPV4_NEXT_HOP);
        currLen = t.getTypeLen(FieldDefinition.IPV4_NEXT_HOP);
        if (currOffset >= 0 && currLen > 0) {
            nexthop = Util.str_addr(next_hop = Util.to_number(buf, off
                    + currOffset, currLen));
        }
        currOffset = t.getTypeOffset(FieldDefinition.INPUT_SNMP);
        currLen = t.getTypeLen(FieldDefinition.INPUT_SNMP);
        if (currOffset >= 0 && currLen > 0) {
            input = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.OUTPUT_SNMP);
        currLen = t.getTypeLen(FieldDefinition.OUTPUT_SNMP);
        if (currOffset >= 0 && currLen > 0) {
            output = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.InPKTS_32);
        currLen = t.getTypeLen(FieldDefinition.InPKTS_32);
        if (currOffset >= 0 && currLen > 0) {
            dPkts = Util.to_number(buf, off + currOffset, currLen) *
                    t.getSamplingRate();
        }
        currOffset = t.getTypeOffset(FieldDefinition.InBYTES_32);
        currLen = t.getTypeLen(FieldDefinition.InBYTES_32);
        if (currOffset >= 0 && currLen > 0) {
            dOctets = Util.to_number(buf, off + currOffset, currLen) *
                      t.getSamplingRate();
        }
        currOffset = t.getTypeOffset(FieldDefinition.FIRST_SWITCHED);
        currLen = t.getTypeLen(FieldDefinition.FIRST_SWITCHED);
        if (currOffset >= 0 && currLen > 0) {
            First = Util.to_number(buf, off + currOffset,
                                   currLen);
            if (!Variation.getInstance().judgeVary(First)) {
                throw new DoneException("Error:Time MisMatch");
            }
        }
        currOffset = t.getTypeOffset(FieldDefinition.LAST_SWITCHED);
        currLen = t.getTypeLen(FieldDefinition.LAST_SWITCHED);
        if (currOffset >= 0 && currLen > 0) {
            try {
                Last = Util.to_number(buf, off + currOffset, currLen);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        currOffset = t.getTypeOffset(FieldDefinition.L4_SRC_PORT);
        currLen = t.getTypeLen(FieldDefinition.L4_SRC_PORT);
        if (currOffset >= 0 && currLen > 0) {
            srcport = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.L4_DST_PORT);
        currLen = t.getTypeLen(FieldDefinition.L4_DST_PORT);
        if (currOffset >= 0 && currLen > 0) {
            dstport = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.TCP_FLAGS);
        currLen = t.getTypeLen(FieldDefinition.TCP_FLAGS);
        if (currOffset >= 0 && currLen > 0) {
            tcp_flags = buf[off + currOffset];
        }
        currOffset = t.getTypeOffset(FieldDefinition.PROT);
        currLen = t.getTypeLen(FieldDefinition.PROT);
        if (currOffset >= 0 && currLen > 0) {
            prot = buf[off + currOffset];
        }
        currOffset = t.getTypeOffset(FieldDefinition.SRC_TOS);
        currLen = t.getTypeLen(FieldDefinition.SRC_TOS);
        if (currOffset >= 0 && currLen > 0) {
            tos = buf[off + currOffset];
        }
        currOffset = t.getTypeOffset(FieldDefinition.SRC_AS);
        currLen = t.getTypeLen(FieldDefinition.SRC_AS);
        if (currOffset >= 0 && currLen > 0) {
            src_as = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.DST_AS);
        currLen = t.getTypeLen(FieldDefinition.DST_AS);
        if (currOffset >= 0 && currLen > 0) {
            dst_as = Util.to_number(buf, off + currOffset, currLen);
        }
        currOffset = t.getTypeOffset(FieldDefinition.SRC_MASK);
        currLen = t.getTypeLen(FieldDefinition.SRC_MASK);
        if (currOffset >= 0 && currLen > 0) {
            src_mask = buf[off + currOffset];
        }
        currOffset = t.getTypeOffset(FieldDefinition.DST_MASK);
        currLen = t.getTypeLen(FieldDefinition.DST_MASK);
        if (currOffset >= 0 && currLen > 0) {
            dst_mask = buf[off + currOffset];
        }
        if (src_addr != 0 || src_mask != 0) {
            srcprefix = new Prefix(src_addr, src_mask);
            dstprefix = new Prefix(dst_addr, dst_mask);
        }
        if (dPkts + dOctets <= 0) { // ���û����
            throw new DoneException("����");
        }
    }

    public V5_Flow(String RouterIP, byte[] buf, int off) throws DoneException {
        this.RouterIP = RouterIP;

        srcaddr = Util.str_addr(src_addr = Util.to_number(buf, off + 0, 4));
        dstaddr = Util.str_addr(dst_addr = Util.to_number(buf, off + 4, 4));
        nexthop = Util.str_addr(next_hop = Util.to_number(buf, off + 8, 4));

        input = Util.to_number(buf, off + 12, 2);
        output = Util.to_number(buf, off + 14, 2);
        dPkts = Util.to_number(buf, off + 16, 4);
        dOctets = Util.to_number(buf, off + 20, 4);
        First = Util.to_number(buf, off + 24, 4);
        Last = Util.to_number(buf, off + 28, 4);
        srcport = Util.to_number(buf, off + 32, 2);
        dstport = Util.to_number(buf, off + 34, 2);
        tcp_flags = buf[off + 37];
        prot = buf[off + 38];
        tos = buf[off + 39];
        src_as = Util.to_number(buf, off + 40, 2);
        dst_as = Util.to_number(buf, off + 42, 2);
        src_mask = buf[off + 44];
        dst_mask = buf[off + 45];

        srcprefix = new Prefix(src_addr, src_mask);
        dstprefix = new Prefix(dst_addr, dst_mask);

            logger.debug("      " + srcaddr + ":"
                              + srcport + " -> " + dstaddr + ":" + dstport +
                              " via "
                              + nexthop);
            logger.debug("        bytes=" + dOctets
                              + ", pkts=" + dPkts + ", proto=" + prot +
                              ", TOS=" + tos
                              + ", TCPF=" + tcp_flags);
            logger.debug("        inIf=" + input
                              + ", outIf=" + output + ", SAS=" + src_as +
                              ", DAS="
                              + dst_as + ", SM=" + src_mask + ", DM=" +
                              dst_mask);
            
        if (dPkts + dOctets <= 0) { // ���û����
            throw new DoneException("����");
        }

    }

    public Long getSrcPort() {
        return new Long(srcport);
    }

    public Long getDstPort() {
        return new Long(dstport);
    }

    public Long getProto() {
        return new Long(prot);
    }

    public Long getTOS() {
        return new Long(tos);
    }

    public Long getSrcAS() {
        return new Long(src_as);
    }

    public Long getDstAS() {
        return new Long(dst_as);
    }

    public Address getSrcAddr() {
        return new Address(src_addr);
    }

    public Address getDstAddr() {
        return new Address(dst_addr);
    }

    public Address getNextHop() {
        return new Address(next_hop);
    }

    public Long getInIf() {
        return new Long(input);
    }

    public Long getOutIf() {
        return new Long(output);
    }

    public Prefix getSrcPrefix() {
        return srcprefix;
    }

    public Prefix getDstPrefix() {
        return dstprefix;
    }

    public void save_raw(long SysUptime, long unix_secs, long unix_nsecs,
                         long flow_sequence, long engine_type, long engine_id,
                         PreparedStatement add_raw_stm) {
        try {
            add_raw_stm.setString(1, RouterIP);
            add_raw_stm.setLong(2, SysUptime);
            add_raw_stm.setLong(3, unix_secs);
            add_raw_stm.setLong(4, unix_nsecs);
            add_raw_stm.setLong(5, flow_sequence);
            add_raw_stm.setInt(6, (int) engine_type);
            add_raw_stm.setInt(7, (int) engine_id);
            add_raw_stm.setString(8, srcaddr);
            add_raw_stm.setString(9, dstaddr);
            add_raw_stm.setString(10, nexthop);
            add_raw_stm.setInt(11, (int) input);
            add_raw_stm.setInt(12, (int) output);
            add_raw_stm.setLong(13, dPkts);
            add_raw_stm.setLong(14, dOctets);
            add_raw_stm.setLong(15, First);
            add_raw_stm.setLong(16, Last);
            add_raw_stm.setInt(17, (int) srcport);
            add_raw_stm.setInt(18, (int) dstport);
            add_raw_stm.setInt(19, (int) tcp_flags);
            add_raw_stm.setInt(20, (int) prot);
            add_raw_stm.setInt(21, (int) tos);
            add_raw_stm.setInt(22, (int) src_as);
            add_raw_stm.setInt(23, (int) dst_as);
            add_raw_stm.setInt(24, (int) src_mask);
            add_raw_stm.setInt(25, (int) dst_mask);
            add_raw_stm.setString(26, Params.getCurrentTime());
            add_raw_stm.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            SQL.error_msg("INSERT to V5 raw table", e, null);
        }
    }

    public void save_raw4v9(long SysUptime, long unix_secs,
                            long packageSequence,
                            long sourceId, PreparedStatement add_raw_stm) {
        try {
            add_raw_stm.setString(1, RouterIP);
            add_raw_stm.setLong(2, SysUptime);
            add_raw_stm.setLong(3, unix_secs);
            add_raw_stm.setLong(4, packageSequence);
            add_raw_stm.setLong(5, sourceId);
            add_raw_stm.setString(6, srcaddr);
            add_raw_stm.setString(7, dstaddr);
            add_raw_stm.setString(8, nexthop);
            add_raw_stm.setInt(9, (int) input);
            add_raw_stm.setInt(10, (int) output);
            add_raw_stm.setLong(11, dPkts);
            add_raw_stm.setLong(12, dOctets);
            add_raw_stm.setLong(13, First);
            add_raw_stm.setLong(14, Last);
            add_raw_stm.setInt(15, (int) srcport);
            add_raw_stm.setInt(16, (int) dstport);
            add_raw_stm.setInt(17, (int) tcp_flags);
            add_raw_stm.setInt(18, (int) prot);
            add_raw_stm.setInt(19, (int) tos);
            add_raw_stm.setInt(20, (int) src_as);
            add_raw_stm.setInt(21, (int) dst_as);
            add_raw_stm.setInt(22, (int) src_mask);
            add_raw_stm.setInt(23, (int) dst_mask);
            add_raw_stm.setString(24, Params.getCurrentTime());
            add_raw_stm.executeUpdate();
        } catch (SQLException e) {
            SQL.error_msg("INSERT to V9 raw table", e, null);
        }
    }

    public Scheme_DataSrcAS getDataSrcAS() {
        return new Scheme_DataSrcAS(RouterIP, 1, 0, // ???
                                    dPkts, dOctets, src_as);
    }

    public Scheme_DataDstAS getDataDstAS() {
        return new Scheme_DataDstAS(RouterIP, 1, 0, // ???
                                    dPkts, dOctets, dst_as);
    }

    public Scheme_DataASMatrix getDataASMatrix() {
        return new Scheme_DataASMatrix(RouterIP, 1, 0, // ???
                                       dPkts, dOctets, src_as, dst_as);
    }

    public Scheme_DataNode getDataSrcNode() {
        return new Scheme_DataNode(RouterIP, 1, 0, // ???
                                   dPkts, dOctets, srcaddr);
    }

    public Scheme_DataNode getDataDstNode() {
        return new Scheme_DataNode(RouterIP, 1, 0, // ???
                                   dPkts, dOctets, dstaddr);
    }

    public Scheme_DataHostMatrix getDataHostMatrix() {
        return new Scheme_DataHostMatrix(RouterIP, 1, 0, // ???
                                         dPkts, dOctets, srcaddr, dstaddr);
    }

    public Scheme_DataInterface getDataSrcInterface() {
        return new Scheme_DataInterface(RouterIP, 1, 0, // ???
                                        dPkts, dOctets, input);
    }

    public Scheme_DataInterface getDataDstInterface() {
        return new Scheme_DataInterface(RouterIP, 1, 0, // ???
                                        dPkts, dOctets, output);
    }

    public Scheme_DataInterfaceMatrix getDataInterfaceMatrix() {
        return new Scheme_DataInterfaceMatrix(RouterIP, 1, 0, // ???
                                              dPkts, dOctets, input, output);
    }

    public Scheme_DataPrefix getDataSrcPrefix() {
        return new Scheme_DataPrefix(RouterIP, 1, 0, // ???
                                     dPkts, dOctets, srcprefix, src_as, input);
    }

    public Scheme_DataPrefix getDataDstPrefix() {
        return new Scheme_DataPrefix(RouterIP, 1, 0, // ???
                                     dPkts, dOctets, dstprefix, dst_as, output);
    }

    public Scheme_DataPrefixMatrix getDataPrefixMatrix() {
        return new Scheme_DataPrefixMatrix(RouterIP, 1, 0, // ???
                                           dPkts, dOctets, srcprefix, src_as,
                                           input, dstprefix, dst_as,
                                           output);
    }

    public Scheme_DataProtocol getDataProtocol() {
        return new Scheme_DataProtocol(RouterIP, 1, 0, dPkts, dOctets, this);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(V5_Flow obj) {
        if ((this.dOctets == obj.dOctets) && (this.dPkts == obj.dPkts)
            && (this.dst_addr == obj.dst_addr)
            && (this.dst_as == obj.dst_as)
            && (this.dst_mask == obj.dst_mask)
            && (this.dstport == obj.dstport) && (this.First == obj.First)
            && (this.input == obj.input) && (this.Last == obj.Last)
            && (this.next_hop == obj.next_hop)
            && (this.output == obj.output) && (this.prot == obj.prot)
            && (this.src_addr == obj.src_addr)
            && (this.src_as == obj.src_as)
            && (this.src_mask == obj.src_mask)
            && (this.srcport == obj.srcport)
            && (this.tcp_flags == obj.tcp_flags) && (this.tos == obj.tos)) {
            return true;
        } else {
            return false;
        }
    }
}
