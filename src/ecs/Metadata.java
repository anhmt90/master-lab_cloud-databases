package ecs;

import util.Validate;

import javax.xml.soap.Node;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Metadata implements Serializable {
    List<NodeInfo> meta = new ArrayList<>();

    /**
     * Adds an element to the metadata list
     *
     * @param nodeName name of the added node
     * @param host     IPv4 address of the host in String format
     * @param port     port on which the ECS connects
     * @param start    the beginning of the key range the server is responsible for
     * @param end      the end of the key range the server is responsible for
     */
    public void add(String nodeName, String host, int port, String start, String end) {
        System.out.println(String.format("WHAT %s %s", start, end));
        NodeInfo kvSMeta = new NodeInfo(nodeName, host, port, start, end);
        this.meta.add(kvSMeta);
    }

    public void add(NodeInfo nodeInfo) {
        meta.add(nodeInfo);
    }

    /**
     * Finds a matching server for a hex key
     *
     * @param hexKey hashed key in hex format
     * @return String containing server address and port
     */
    public NodeInfo findMatchingServer(String hexKey) {
        for (NodeInfo nodeInfo : meta) {
            if (nodeInfo.getRange().inRange(hexKey)) {
                return nodeInfo;
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

    public List<NodeInfo> get() {
        return meta;
    }
}
