package ecs;

public class NodeInfo {
    private String name;
    private String host;
    private int port;
    private KeyHashRange range;

    public NodeInfo(String nodeName, String host, int port, String start, String end) {
        this(nodeName, host, port, new KeyHashRange(start, end));
    }

    public NodeInfo(String nodeName, String host, int port, KeyHashRange range) {
        this.name = nodeName;
        this.host = host;
        this.port = port;
        this.range = range;
    }

    public String getName() {
        return name;
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
