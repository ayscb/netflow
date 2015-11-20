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
package cai.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;

import cai.utils.DoneException;
import cai.utils.Resources;
import cai.utils.SuperString;
import com.javaforge.styx.utils.AppConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

public class SQL {
    //TODO Add DB user and password configurations in the configuration file.
        static Configuration config = AppConfiguration.getConfig();
        
        static Logger logger = Logger.getLogger(cai.sql.SQL.class);
        
        //static public Resources resources = new Resources("SQL");

        static boolean JDBC_abort_on_error = config.getBoolean("database.abortOnError");

	private Hashtable prep_stms = new Hashtable();

	public static final int MAX_CONN = config.getInt("database.maxPoolConnections");

	public static Connection[] ConPool = new Connection[MAX_CONN];

	public static void error_msg(String msg, SQLException e, String stm)
			throws RuntimeException {
		String s = new String("SQL: " + msg + ": " + e.getErrorCode() + "/"
				+ e.getSQLState() + " " + e.getMessage());

		if (stm != null)
			s += "\nSQL: Statement `" + stm + "'";

                logger.error(s);

		if (JDBC_abort_on_error)
			throw new RuntimeException(s);
	}

	static String SQLURI = config.getString("database.uri");

	static String SQLdriverName = config.getString("database.driver");;
	static {
		try {
                        logger.debug("Loading DB with driver: " + SQLdriverName);
			Class.forName(SQLdriverName).newInstance();
		} catch (Exception e) {
			throw new DoneException("SQL: Unable to load " + SQLdriverName
					+ ": " + SuperString.exceptionMsg(e.toString()));
		}
                
                logger.debug("Creating Connection Pool of " + MAX_CONN
                                        + " connections using URI: " + SQLURI);
		for (int conIdx = 0; conIdx < MAX_CONN; conIdx++) {
			try {
				ConPool[conIdx] = DriverManager.getConnection(SQLURI);
			} catch (SQLException e) {
				error_msg("Cannot to connect", e, null);
			}
		}
	}

	private static int ConIdx = 0;

	public static Connection getConn() {
		return ConPool[(ConIdx++) % MAX_CONN];
	}

	private Connection connection = null;

	public SQL() throws DoneException {
		connection = getConn();

	}

	public ResultSet execToken(String msg, String token) throws DoneException {
		String sqlStr = config.getString(token);
		return exec(msg, sqlStr);
	}

	public ResultSet exec(String msg, String stm) throws DoneException {
		ResultSet re = null;

		try {
			if (stm.trim().toUpperCase().startsWith("SELECT")) {
				Statement stmt = connection.createStatement();
				re = stmt.executeQuery(stm);
			} else {
				try {
					connection.createStatement().execute(stm);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
                        System.out.println("What the.....");
			error_msg(msg, e, stm);
                        e.printStackTrace();
		}

		return re;
	}

	public ResultSet exec(String msg, PreparedStatement stm)
			throws DoneException {
		ResultSet re = null;

		try {
			re = stm.executeQuery();
		} catch (SQLException e) {
			error_msg(msg, e, null);
		}

		return re;
	}

    @SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String msg, String stm) {
		Integer hash = new Integer(Thread.currentThread().hashCode()
				^ stm.hashCode() ^ msg.hashCode());// ȡΨһֵ
		PreparedStatement st = (PreparedStatement) prep_stms.get(hash);

		if (st != null)
			return st;

		try {
			st = connection.prepareStatement(stm);
		} catch (SQLException e) {
			error_msg(msg, e, stm);
		}

		if (st != null)
			prep_stms.put(hash, st);

		return st;
	}

	public void create_DB() {
		exec("Create IpSegments", config.getString("SQL.Create.IpSegments"));
		exec("Create V1 raw table", config.getString("SQL.Create.RawV1"));
		exec("Create V5 raw table", config.getString("SQL.Create.RawV5"));
		exec("Create V7 raw table", config.getString("SQL.Create.RawV7"));
		exec("Create V8 AS raw table", config.getString("SQL.Create.RawV8.AS"));
		exec("Create V8 ProtoPort raw table", config.getString("SQL.Create.RawV8.ProtoPort"));
		exec("Create V8 DstPrefix raw table", config.getString("SQL.Create.RawV8.DstPrefix"));
		exec("Create V8 SrcPrefix raw table", config.getString("SQL.Create.RawV8.SrcPrefix"));
		exec("Create V8 Prefix raw table", config.getString("SQL.Create.RawV8.Prefix"));
		exec("Create V9 raw table", config.getString("SQL.Create.RawV9"));
		exec("Create option table", config.getString("SQL.Create.OptionsTable"));
		exec("Create SrcAS table", config.getString("SQL.Create.SrcAS"));
		exec("Create DstAS table", config.getString("SQL.Create.DstAS"));
		exec("Create ASMatrix table", config.getString("SQL.Create.ASMatrix"));
		exec("Create SrcNode table", config.getString("SQL.Create.SrcNode"));
		exec("Create DstNode table", config.getString("SQL.Create.DstNode"));
		exec("Create HostMatrix table", config.getString("SQL.Create.HostMatrix"));
		exec("Create SrcInterface table", config.getString("SQL.Create.SrcInterface"));
		exec("Create DstInterface table", config.getString("SQL.Create.DstInterface"));
		exec("Create InterfaceMatrix table", config.getString("SQL.Create.InterfaceMatrix"));
		exec("Create SrcPrefix table", config.getString("SQL.Create.SrcPrefix"));
		exec("Create DstPrefix table", config.getString("SQL.Create.DstPrefix"));
		exec("Create PrefixMatrix table", config.getString("SQL.Create.PrefixMatrix"));
		exec("Create Protocol table", config.getString("SQL.Create.Protocol"));
	}

	public void delete_DB() {
		// exec("Remove IpSegments",
		// config.getString("SQL.Drop.IpSegments"));

		exec("Remove V1 raw table", config.getString("SQL.Drop.RawV1"));
		exec("Remove V5 raw table", config.getString("SQL.Drop.RawV5"));
		exec("Remove V7 raw table", config.getString("SQL.Drop.RawV7"));
		exec("Remove V8 AS raw table", config.getString("SQL.Drop.RawV8.AS"));
		exec("Remove V8 ProtoPort raw table", config.getString("SQL.Drop.RawV8.ProtoPort"));
		exec("Remove V8 DstPrefix raw table", config.getString("SQL.Drop.RawV8.DstPrefix"));
		exec("Remove V8 SrcPrefix raw table", config.getString("SQL.Drop.RawV8.SrcPrefix"));
		exec("Remove V8 Prefix raw table", config.getString("SQL.Drop.RawV8.Prefix"));
		exec("Remove V9 raw table", config.getString("SQL.Drop.RawV9"));
		exec("Remove option table", config.getString("SQL.Drop.OptionsTable"));
		exec("Remove SrcAS table", config.getString("SQL.Drop.SrcAS"));
		exec("Remove DstAS table", config.getString("SQL.Drop.DstAS"));
		exec("Remove ASMatrix table", config.getString("SQL.Drop.ASMatrix"));
		exec("Remove SrcNode table", config.getString("SQL.Drop.SrcNode"));
		exec("Remove DstNode table", config.getString("SQL.Drop.DstNode"));
		exec("Remove HostMatrix table", config.getString("SQL.Drop.HostMatrix"));
		exec("Remove SrcInterface table", config.getString("SQL.Drop.SrcInterface"));
		exec("Remove DstInterface table", config.getString("SQL.Drop.DstInterface"));
		exec("Remove InterfaceMatrix table", config.getString("SQL.Drop.InterfaceMatrix"));
		exec("Remove SrcPrefix table", config.getString("SQL.Drop.SrcPrefix"));
		exec("Remove DstPrefix table", config.getString("SQL.Drop.DstPrefix"));
		exec("Remove PrefixMatrix table", config.getString("SQL.Drop.PrefixMatrix"));
		exec("Remove Protocol table", config.getString("SQL.Drop.Protocol"));
	}
}

// while( rez.next() )
// System.out.println( rez.getString( "NUM" )+" "+rez.getString( "FIRST" )+
// " "+rez.getString( "SECOND" ) );
