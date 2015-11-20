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
// 2007 Nov 12 - Replaced syslog object with log4j entries and added descriptive
//  log entries.
// 2007 Dec 2 - Removed ResourceBundle objects used for configuration, and replaced
//  with Apache Commons Configuration object for flexibilty and ease.
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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import cai.flow.collector.interpretator.IpSegmentManager;
import cai.flow.packets.FlowPacket;
import cai.flow.packets.V1_Packet;
import cai.flow.packets.V5_Packet;
import cai.flow.packets.V7_Packet;
import cai.flow.packets.V8_Packet;
import cai.flow.packets.V9_Packet;
import cai.utils.DoneException;
import cai.utils.Params;
import cai.utils.Resources;
import cai.utils.ServiceThread;
import cai.utils.SuperString;
import cai.utils.Syslog;
import cai.utils.Util;
import com.javaforge.styx.utils.AppConfiguration;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
class Collector {
        static Configuration config = AppConfiguration.getConfig();
                
	static Resources resources;

	static InetAddress localHost;
        
        static Logger logger = Logger.getLogger(Collector.class.getName());

	static int localPort;

	static int receiveBufferSize;

	static boolean[] isVersionEnabled;

	static int max_queue_length;

	static int collector_thread;

	static final int MAX_VERION = 9;

	static Hashtable routers;

	static {
		IpSegmentManager.getInstance();

                receiveBufferSize = config.getInt("net.receive.buffer.size");
                
                localPort = config.getInt("net.bind.port");
                
                String local = config.getString("net.bind.host");
		
                Params.v9TemplateOverwrite = config.getBoolean("flow.collector.V9.template.overwrite");
		
                Params.template_refreshFromHD = config.getBoolean("flow.collector.template.refreshFromHD");

                Params.ip2ipsConvert = config.getBoolean("flow.ip2ipsConvert");
                
                String[] ipSrcEx = config.getStringArray("ip.source.excludes");
                
                String[] ipSrcIn = config.getStringArray("ip.source.includes");
                
		int idxOfEx = 0;
                Params.ipSrcExcludes = new long[ipSrcEx.length];
                Params.ipSrcIncludes = new long[ipSrcIn.length];
                
                // TODO - Cosmetic - Rather than create an index of array to store and
                //  track ipSrcExcludes ourselves, create a List/Hashtable-type object to "add" or "remove"
                //  values.  Something like ipSrcExcludes.add(tmpl)
                for (String excludeIp : ipSrcEx) {
                    long tmpl = Util.convertIPS2Long(excludeIp);
                    Params.ipSrcExcludes[idxOfEx++] = tmpl;
                }
                
		int idxOfIn = 0;
                
                // TODO  - Cosmetic - Rather than create an index of array to store and
                //  track ipSrcIncludes ourselves, create a List/Hashtable-type object to "add" or "remove"
                //  values.  Something like ipSrcIncludes.add(tmpl)
                for (String includeIp : ipSrcIn) {
                    long tmpl = Util.convertIPS2Long(includeIp);
                    Params.ipSrcIncludes[idxOfIn++] = tmpl;
                }
                
                String[] ipDstEx = config.getStringArray("ip.dst.excludes");
                String[] ipDstIn = config.getStringArray("ip.dst.includes");
                
		Params.ipDstExcludes=new long[ipDstEx.length];
		idxOfEx = 0;
                
                // TODO  - Cosmetic - Rather than create an index of array to store and
                //  track ipSrcIncludes ourselves, create a List/Hashtable-type object to "add" or "remove"
                //  values.  Something like ipDstExcludes.add(tmpl) 
                for (String excludeIp : ipDstEx) {
                    long tmpl = Util.convertIPS2Long(excludeIp);
                    Params.ipDstExcludes[idxOfEx++] = tmpl;
                }
                
		Params.ipDstIncludes=new long[ipDstIn.length];
		idxOfIn = 0;
                
                // TODO  - Cosmetic - Rather than create an index of array to store and
                //  track ipSrcIncludes ourselves, create a List/Hash-type object to "add" or "remove"
                //  values.  Something like ipDstIncludes.add(tmpl)
                for (String includeIp : ipDstIn) {
                    long tmpl = Util.convertIPS2Long(includeIp);
                    Params.ipDstIncludes[idxOfIn++] = tmpl;
                }

		if (local.equals("any"))
			localHost = null;
		else {
			try {
				localHost = InetAddress.getByName(local);
			} catch (UnknownHostException e) {
				localHost = null;
			}

			if (localHost == null)
				resources.error("unknown host `" + local + "'");
		}

		isVersionEnabled = new boolean[MAX_VERION];
		isVersionEnabled[0] = config.getBoolean("flow.collector.V1.enabled");
		isVersionEnabled[1] = false;
		isVersionEnabled[2] = false;
		isVersionEnabled[3] = false;
		isVersionEnabled[4] = config.getBoolean("flow.collector.V5.enabled");
		isVersionEnabled[5] = false;
		isVersionEnabled[6] = config.getBoolean("flow.collector.V7.enabled");
		isVersionEnabled[7] = config.getBoolean("flow.collector.V8.enabled");
		isVersionEnabled[8] = config.getBoolean("flow.collector.V9.enabled");

		max_queue_length = config.getInt("flow.collector.max_queue_length");
		collector_thread = config.getInt("flow.collector.collector.thread");

		if (collector_thread < 1)
			resources.error("key `" + collector_thread + "' bust be great one");

                // Determine and build the router groups
                
		routers = new Hashtable();
                
                int groupNum = config.getList("routers.group[@address]").size();
                logger.debug("Found " + groupNum + " router groups defined");
                // Iterate over each router group
                for (int i=0; i < groupNum; i++) {
                    InetAddress router_group = null;
                    boolean putted = false;
                    try {
                        router_group = InetAddress.getByName(config.getString("routers.group(" + i + ")[@address]"));
                    } catch (UnknownHostException e1) {
                        logger.error("Unknown host:" + config.getString("routers.group(" + i + ")[@address]"));
                    }
                    
                    int routerNum = config.getList("routers.group(" + i + ").router").size();
                    logger.debug("Found " + routerNum + " routers defined in all groups");
                    // Iterate over each router inside the router group
                    for (int j=0; j < routerNum; j++) {
                        InetAddress router = null;
                        try {
                            router = InetAddress.getByName(config.getString("routers.group(" + i + ").router(" + j + ")"));
                        } catch (UnknownHostException e2) {
                            logger.error("Unknown host:" + config.getString("routers.group(" + i + ").router(" + j + ")"));
                        }
                        // Place the router in the group
                        logger.debug("Adding router: " + router.toString() + " to the group: " + router_group.toString());
                        routers.put(router, router_group);
                        putted = true;
                    }
                    if (!putted) {
                        logger.warn("No routers in the group: " + router_group.toString());
                    }
                }
                /*
		ResourceBundle bundle = resources.getResourceBundle();
		String prefix = "flow.collector.router.group.";
		int prefix_len = prefix.length();

		for (Enumeration e = bundle.getKeys(); e.hasMoreElements();) {
			String entry = (String) e.nextElement();

			if (!entry.startsWith(prefix))
				continue;

			InetAddress router_group = null;
			boolean putted = false;

			try {
				router_group = InetAddress.getByName(entry
						.substring(prefix_len));
			} catch (UnknownHostException e1) {
				resources.error("unknown host `" + entry.substring(prefix_len)
						+ "' in `" + entry + "'");
			}

			String the_routers = bundle.getString(entry);

			for (StringTokenizer st = new StringTokenizer(the_routers); st
					.hasMoreElements();) {
				String router_name = st.nextToken();
				InetAddress router = null;

				try {
					router = InetAddress.getByName(router_name);
				} catch (UnknownHostException e2) {
					resources.error("unknown host `" + router_name + "' in `"
							+ entry + "'");
				}

				routers.put(router, router_group);
				putted = true;
			}

			if (!putted)
				resources.error("key `" + the_routers
						+ "' -- no routers in group");
		}
                */
	}

	//Syslog syslog;

	LinkedList data_queue;

	Aggregate aggregator;

	long queued = 0, processed = 0;

	int sampleRate = 1;

	int stat_interval;

	public Collector() {
                sampleRate = config.getInt("sample.rate");
		if (sampleRate == 0) {
			sampleRate = 1;
		}
                stat_interval = Util.getInterval(config.getString("flow.collector.statistics.interval"));

                //syslog = new Syslog("NetFlow", logOptions, logFacility);
		//syslog.setlogmask(Syslog.LOG_UPTO(logLevel));
		//syslog.syslog(Syslog.LOG_DEBUG, "Syslog created: " + syslog.toString());
                
		aggregator = new Aggregate(resources);// ���еĹ鲢�̺߳�SQL
		data_queue = new LinkedList();
	}

	/**
	 * ���ÿ��Ժ���Ҫ����������
	 *
	 */
	void go() {

                ServiceThread rdr = new ServiceThread(this,"Reader at " 
                                + (localHost == null ? "any" : "" + localHost) + ":"
                                + localPort, "REader") {
                        public void exec() throws Throwable {
                            ((Collector) o).reader_loop();
                        }
                };
                rdr.setName(rdr.getName()+"-Reader");
		rdr.setPriority(Thread.MAX_PRIORITY);
		rdr.setDaemon(true);
		rdr.start();

		ServiceThread statistics;
		/**
		 */
		if (stat_interval != 0) {
			statistics = new ServiceThread(this, "Statistics over "
					+ Util.toInterval(stat_interval), "Statistics") {
				public void exec() throws Throwable {
					((Collector) o).statistics_loop();
				}
			};
                        statistics.setName(statistics.getName()+"-Statistics");
			statistics.setDaemon(true);
			statistics.start();
		}

		ServiceThread[] cols = new ServiceThread[collector_thread];

		for (int i = 0; i < collector_thread; i++) {
			String title = new String("Collector" + (i + 1));
			ServiceThread col = new ServiceThread(this, title, title) {
				public void exec() {
					((Collector) o).collector_loop();
				}
			};
  
			cols[i] = col;
                        col.setName(col.getName()+"-"+title);
			col.start();
		}

		try {
			for (int i = 0; i < collector_thread; i++)
				cols[i].join();
		} catch (InterruptedException e) {
                        logger.fatal("Collector - InterruptedException in main thread, exit");
                        logger.debug(e.getMessage());
		}
	}

	/**
	 * ͳ���̵߳����
	 *
	 * @throws Throwable
	 */
	public void statistics_loop() throws Throwable {
		long start = System.currentTimeMillis();

		while (true) {
			try {
				Thread.sleep(stat_interval * 1000);
			} catch (InterruptedException e) {
			}

			long u = System.currentTimeMillis() - start;
			String s = "" + ((float) queued * 1000 / u);
			int i = s.indexOf('.') + 3;

			if (i < s.length())
				s = s.substring(0, i);
                        logger.info("Pkts " + queued + "/" + processed
					+ ", " + s + " pkts/sec, " + Util.uptime_short(u / 1000));

		}
	}
	SampleManager sampler = null;
	{
		sampler = new SampleManager(sampleRate);
	}

	/**
	 *
	 * @throws Throwable
	 */
	public void reader_loop() throws Throwable {
		DatagramSocket socket;

		try {
			try {
				socket = new DatagramSocket(localPort, localHost);
				socket.setReceiveBufferSize(receiveBufferSize);
			} catch (IOException exc) {
                                logger.fatal("Reader - socket create error: "
                                        + localHost + ":" + localPort);
                                logger.debug(exc);
				throw exc;
			}

			while (true) {
				byte[] buf = new byte[2048];// Ч�������ط��������
				DatagramPacket p = null;
				if (p == null) {
					p = new DatagramPacket(buf, buf.length);

					try {
						socket.receive(p);
					} catch (IOException exc) {
                                                logger.error("Reader - socket read error: " + exc.getMessage());
                                                logger.debug (exc);
						put_to_queue(null);// ��ʾnotifyAll
						break;
					}
				}

				if (this.sampler.shouldDue()) {
					put_to_queue(p);
				}
				p = null;
			}
		} catch (Throwable e) {
			logger.error("Exception trying to abort collector");
			put_to_queue(null);
			throw e;
		}
	}

	/**
	 * UDP��Ļ���
	 *
	 * @param p
	 */
	void put_to_queue(final DatagramPacket p) {
		InetAddress router = p.getAddress();
		InetAddress group = (InetAddress) routers.get(router);

		if (group == null) {
                        logger.warn("A packet from an unauthorized device is ignored.  Device: " + router);
			return;
		}

                logger.debug("Packet from device " + router + " is moved to group "
                        + group);
		p.setAddress(group);// ����ʵrouter�ĵ�ַ�ĳ�group�ĵ�ַ

		if (data_queue.size() > max_queue_length) {
                        logger.warn("Reader - the queue is bigger than max_queue_length: "
                                + data_queue.size() + "/" + max_queue_length);
                }

		synchronized (data_queue) {
			data_queue.addLast(p);
			queued++;

			if (p == null)
				data_queue.notifyAll();// ������ˣ���ô
			else
				data_queue.notify();// ����
		}
	}

	/**
	 *
	 */
	void collector_loop() {
		boolean no_data = true;

		while (true) {
			Object p = null;

			synchronized (data_queue) {
				try {
					if (data_queue.getFirst() != null)
						p = data_queue.removeFirst();// ȡ���һ��UDP��

					no_data = false;
				} catch (NoSuchElementException ex) {
				}
			}

			if (no_data) {
				synchronized (data_queue) {
					try {
						data_queue.wait();// �ȴ�reader_loop notify
					} catch (InterruptedException e) {
					}
				}
			} else {
				no_data = true;

				if (p == null)
					break;

				processPacket((DatagramPacket) p);
			}
		}
	}

	/**
	 *
	 * @param p
	 */
	private synchronized void processPacket(final DatagramPacket p) {
		final byte[] buf = p.getData();
		int len = p.getLength();
		String addr = p.getAddress().getHostAddress().trim();

		synchronized (data_queue) {
			processed++;
		}

                logger.debug(addr + "(" + p.getAddress().getHostName() + ") "
                        + len + " bytes");
		try {
			if (len < 2)
				throw new DoneException("  * too short packet *");

			short version = (short) Util.to_number(buf, 0, 2);

			if (version > MAX_VERION || version <= 0)
				throw new DoneException("  * unsupported version *");

			if (!isVersionEnabled[version - 1])
				throw new DoneException("  * version " + version
						+ " disabled *");

                        logger.debug("  version: " + version);

			FlowPacket packet;

			switch (version) {
			case 1:
				packet = (FlowPacket) new V1_Packet(addr, buf, len);
				break;
			case 5:
				packet = (FlowPacket) new V5_Packet(addr, buf, len);
				break;
                        // Temporarily removing NetFlow Version 7 until Resources fully removed
                        /*
			case 7:
				packet = (FlowPacket) new V7_Packet(addr, resources, buf, len);
				break;
                        */
			case 8:
				packet = (FlowPacket) new V8_Packet(addr, buf, len);
				break;
			case 9:
				packet = (FlowPacket) new V9_Packet(addr, buf, len);
				break;
			default:
                                logger.error("Collector - Version problem.  Version = " + version);
				return;
			}
			aggregator.process(packet);
		} catch (DoneException e) {
                        e.printStackTrace();
                        logger.info(e);
		}
	}
}
