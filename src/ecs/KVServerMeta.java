package ecs;

public class KVServerMeta {
	private String host;
	private int port;
	private KeyHashRange range;

	public KVServerMeta(String host, int port, String start, String end) {
		this(host, port, new KeyHashRange(start, end));
	}

	public KVServerMeta(String host, int port, KeyHashRange range) {
		this.host = host;
		this.port = port;
		this.range = range;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public KeyHashRange getRange() {
		return range;
	}
}
