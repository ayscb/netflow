package cai.flow.packets;

import cai.flow.struct.Address;
import cai.flow.struct.Prefix;

abstract public class Flow {
	public Long getSrcPort() {
		return null;
	}

	public Long getDstPort() {
		return null;
	}

	public Long getProto() {
		return null;
	}

	public Long getTOS() {
		return null;
	}

	public Long getSrcAS() {
		return null;
	}

	public Long getDstAS() {
		return null;
	}

	public Address getSrcAddr() {
		return null;
	}

	public Address getDstAddr() {
		return null;
	}

	public Address getNextHop() {
		return null;
	}

	public Long getInIf() {
		return null;
	}

	public Long getOutIf() {
		return null;
	}

	public Prefix getSrcPrefix() {
		return null;
	}

	public Prefix getDstPrefix() {
		return null;
	}
}
