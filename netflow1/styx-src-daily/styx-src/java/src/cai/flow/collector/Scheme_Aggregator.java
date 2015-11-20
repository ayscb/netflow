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
// 2007 Nov 09 - No longer using Syslog.  Added more descriptive thread names
// 2007 Dec 02 - No longer using ResourceBundles for Configuration.
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import cai.flow.packets.FlowPacket;
import cai.sql.SQL;
import cai.utils.ServiceThread;
import cai.utils.Syslog;
import cai.utils.Util;
import com.javaforge.styx.utils.AppConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
/**
 * ���й鲢�ĸ���
 * @author CaiMao
 *
 */
public abstract class Scheme_Aggregator extends Hashtable {
    
        static Configuration config = AppConfiguration.getConfig();
    
        static Logger logger = Logger.getLogger(Scheme_Aggregator.class.getName());
        
	long Start, Stop;

	long interval;

	ServiceThread th;

	SQL sql;

	String scheme;

	protected PreparedStatement add_stm = null;

	protected String add_sql = null;

	public Scheme_Aggregator(SQL sql, String scheme, long interval) {
		this.scheme = scheme;//like "SrcAS"
		this.interval = interval;

		if (interval != 0) {
			add_sql = config.getString("SQL.Add." + scheme);
			add_stm = sql.prepareStatement("׼������" + scheme
					+ "��", add_sql);
			//����߳�
			//th = new ServiceThread(this, Syslog.log, "Scheme " + scheme
			//		+ " with " + Util.toInterval(interval) + " interval",
			//		scheme) {
			//	public void exec() throws Throwable {
			//		((Scheme_Aggregator) o).save_loop();
			//	}
                        th = new ServiceThread(this,"Scheme " + scheme
                                        + " with " + Util.toInterval(interval) + " interval",
                                        scheme) {
                                public void exec() throws Throwable {
                                        ((Scheme_Aggregator) o).save_loop();
                                }
			};
                        th.setName(th.getName()+"-"+scheme);
			th.start();
		} else {
			//Syslog.log.syslog(Syslog.LOG_NOTICE, "Scheme " + scheme
			//		+ " disabled");
                    logger.info("Scheme " + scheme + " disabled");
                }
	}
	/**
	 * ��������add(FlowPacket packet)�е���
	 * @param it
	 */
    @SuppressWarnings("unchecked")
	public void add(Scheme_Item it) {
		Integer hash = new Integer(it.hashCode());

		if (it.getData().RouterIP == null)
			throw new RuntimeException("it.getData().RouterIP == null for "
					+ it.toString());

		synchronized (this) {
			Object o = get(hash);

			if (o == null)
				put(hash, it);//������scheme item���б���
			else
				((Scheme_Item) o).add(it);//��������
		}
	}
	/**
	 * ��collector4�İ������
	 * @param packet
	 */
	public abstract void add(FlowPacket packet);

	private void init_times() {
		Start = System.currentTimeMillis() / 1000;
		Stop = Start + interval;
	}
	/**
	 * ����̵߳���ѭ��
	 *
	 */
	public void save_loop() {
		init_times();//��ϵͳʱ�����

		while (true) {
			try {
				long wait = Stop - Start;

				if (wait >= 0)
					Thread.sleep(wait * 1000);

				synchronized (this) {//��addͬ��
					for (Enumeration f = elements(); f.hasMoreElements();) {
						Scheme_Item item = (Scheme_Item) f.nextElement();

						try {
							//ͷ�ĸ��ֶ��ǹ̶���
							add_stm.setDate(1, new java.sql.Date(Start * 1000));
							add_stm.setTime(2, new java.sql.Time(Start * 1000));
							add_stm.setDate(3, new java.sql.Date(Stop * 1000));
							add_stm.setTime(4, new java.sql.Time(Stop * 1000));
							item.fill(add_stm, 5);
							add_stm.executeUpdate();
						} catch (SQLException e) {
							SQL.error_msg("INSERT to " + scheme + " table", e,
									add_sql);
						}
					}

					clear();//������Ժ����Hashtable
				}

				init_times();
			} catch (InterruptedException e) {
			}
		}
	}

}
