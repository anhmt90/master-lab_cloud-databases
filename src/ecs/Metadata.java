package ecs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Metadata implements Serializable {
	List<KVServerMeta> meta = new ArrayList<>();

	public void add(String host, int port, String start, String end) {
		KVServerMeta kvSMeta = new KVServerMeta(host, port, start, end);
		this.meta.add(kvSMeta);
	}

	/**
	 * Finds a matching server for a hex key
	 * 
	 * @param hexKey hashed key in hex format
	 * @return String containing server address and port
	 */
	public KVServerMeta findMatchingServer(String hexKey) {
		for (KVServerMeta temp : meta) {
			if (temp.getRange().inRange(hexKey)) {
				return temp;
			}
		}
		return null;
	}

	/**
	 * get metadata size
	 * 
	 * @return metadata size
	 */
	public int getSize() {
		return meta.size();
	}

	public List<KVServerMeta> get() {
		return meta;
	}
	
	
}
