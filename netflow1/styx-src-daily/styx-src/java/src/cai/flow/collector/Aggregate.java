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
// 2007-12-05 - Added Logging
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

import java.util.Iterator;
import java.util.LinkedList;

import cai.flow.packets.FlowPacket;
import cai.sql.SQL;
import cai.utils.DoneException;
import cai.utils.Resources;
import cai.utils.Util;
import com.javaforge.styx.utils.AppConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

public class Aggregate extends LinkedList {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9049626103334099526L;
        
        Configuration config = AppConfiguration.getConfig();
        static Logger logger = Logger.getLogger(Aggregate.class.getName());

	SQL sql;

	boolean save_raw_flow;
	/**
	 * ���еĹ鲢��������֯
	 * @param resources
	 * @throws DoneException
	 */
    @SuppressWarnings("unchecked")
	public Aggregate(Resources resources) throws DoneException {
		sql = new SQL();

		save_raw_flow = config.getBoolean("flow.collector.aggregate.raw.enabled");

		long interval;
		//�ȷ����������õĹ鲢�߳�
                if ((interval = Util.getInterval(config.getString("flow.collector.SrcAS.interval"))) !=0) {
                    add(new Scheme_AggregatorSrcAS(sql, interval));
                } /*
		if ((interval = resources.getInterval("flow.collector.SrcAS.interval")) != 0)
			add(new Scheme_AggregatorSrcAS(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.DstAS.interval"))) !=0) {
                    add(new Scheme_AggregatorDstAS(sql, interval));
                } /*
		if ((interval = resources.getInterval("flow.collector.DstAS.interval")) != 0)
			add(new Scheme_AggregatorDstAS(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.ASMatrix.interval"))) !=0) {
                    add(new Scheme_AggregatorASMatrix(sql, interval));
                } /*
		if ((interval = resources
				.getInterval("flow.collector.ASMatrix.interval")) != 0)
			add(new Scheme_AggregatorASMatrix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.SrcNode.interval"))) !=0) {
                    add(new Scheme_AggregatorSrcNode(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.SrcNode.interval")) != 0)
			add(new Scheme_AggregatorSrcNode(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.DstNode.interval"))) !=0) {
                    add(new Scheme_AggregatorDstNode(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.DstNode.interval")) != 0)
			add(new Scheme_AggregatorDstNode(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.HostMatrix.interval"))) !=0) {
                    add(new Scheme_AggregatorHostMatrix(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.HostMatrix.interval")) != 0)
			add(new Scheme_AggregatorHostMatrix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.SrcInterface.interval"))) !=0) {
                    add(new Scheme_AggregatorSrcInterface(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.SrcInterface.interval")) != 0)
			add(new Scheme_AggregatorSrcInterface(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.DstInterface.interval"))) !=0) {
                    add(new Scheme_AggregatorDstInterface(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.DstInterface.interval")) != 0)
			add(new Scheme_AggregatorDstInterface(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.InterfaceMatrix.interval"))) !=0) {
                    add(new Scheme_AggregatorInterfaceMatrix(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.InterfaceMatrix.interval")) != 0)
			add(new Scheme_AggregatorInterfaceMatrix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.SrcPrefix.interval"))) !=0) {
                    add(new Scheme_AggregatorSrcPrefix(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.SrcPrefix.interval")) != 0)
			add(new Scheme_AggregatorSrcPrefix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.DstPrefix.interval"))) !=0) {
                    add(new Scheme_AggregatorDstPrefix(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.DstPrefix.interval")) != 0)
			add(new Scheme_AggregatorDstPrefix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.PrefixMatrix.interval"))) !=0) {
                    add(new Scheme_AggregatorPrefixMatrix(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.PrefixMatrix.interval")) != 0)
			add(new Scheme_AggregatorPrefixMatrix(sql, interval));
                */
                if ((interval = Util.getInterval(config.getString("flow.collector.Protocol.interval"))) !=0) {
                    add(new Scheme_AggregatorProtocol(sql, interval));
                }/*
		if ((interval = resources
				.getInterval("flow.collector.Protocol.interval")) != 0)
			add(new Scheme_AggregatorProtocol(sql, interval));
                */
	}

	public void process(final FlowPacket packet) {
		if (save_raw_flow)
                    logger.debug("Processing Raw Packet");
			packet.process_raw(sql);

		for (Iterator list = iterator(); list.hasNext();)
			((Scheme_Aggregator) list.next()).add(packet);
	}

}
