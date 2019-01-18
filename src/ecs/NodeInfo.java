package ecs;

import java.io.Serializable;

/**
 * Metadata of a particular server such as server id, address, port and write range
 */
public class NodeInfo implements Serializable {
    /**
     * Unique id to identify the node from other nodes
     */
    private String id;

    /**
     * Serive IPv4 address
     */
    private String host;

    /**
     * Service port
     */
    private int port;

    private KeyHashRange writeRange;

    public NodeInfo(String nodeName, String host, int port, String start, String end) {
        this(nodeName, host, port, new KeyHashRange(start, end));
    }

    public NodeInfo(String nodeName, String host, int port, KeyHashRange writeRange) {
        this.id = nodeName;
        this.host = host;
        this.port = port;
        this.writeRange = writeRange;
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

    public KeyHashRange getWriteRange() {
        return writeRange;
    }

    @Override
    public String toString() {
        return "\nNodeInfo{" +
                "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", writeRange=" + writeRange +
                "}\n";
    }
}
