package cai.flow.collector;

import java.util.Enumeration;
import java.util.Vector;

import cai.flow.packets.FlowPacket;
import cai.flow.struct.Scheme_DataASMatrix;
import cai.sql.SQL;

public class Scheme_AggregatorASMatrix extends Scheme_Aggregator {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7036996233352475701L;

	public Scheme_AggregatorASMatrix(SQL sql, long interval) {
		super(sql, "ASMatrix", interval);
	}

	public void add(FlowPacket packet) {
		Vector v = packet.getASMatrixVector();

		if (v == null)
			return;

		for (Enumeration f = v.elements(); f.hasMoreElements();)
			add(new Scheme_ItemASMatrix((Scheme_DataASMatrix) f.nextElement()));
	}

}
