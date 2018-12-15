package ecs;

import java.io.Serializable;

/**
 * Contains important info for a server like address, port and KeyHashRange
 *
 */
public class NodeInfo implements Serializable {
    private String id;
    private String host;
    private int port;
    private KeyHashRange range;

    public NodeInfo(String nodeName, String host, int port, String start, String end) {
        this(nodeName, host, port, new KeyHashRange(start, end));
    }

    public NodeInfo(String nodeName, String host, int port, KeyHashRange range) {
        this.id = nodeName;
        this.host = host;
        this.port = port;
        this.range = range;
    }

    public NodeInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getId() {
        return id;
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

    @Override
    public String toString() {
        return "\nNodeInfo{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", range=" + range +
                "}\n";
    }
}
