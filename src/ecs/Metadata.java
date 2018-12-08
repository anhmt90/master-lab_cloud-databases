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
//        System.out.println(String.format("add RANGE %s %s", start, end));
        NodeInfo nodeInfo = new NodeInfo(nodeName, host, port, start, end);
        meta.add(nodeInfo);
    }

    public void add(NodeInfo nodeInfo) {
        meta.add(nodeInfo);
    }

    /**
     * Finds a matching server for a hex key
     *
     * @param keyHashed hashed key in hex format
     * @return String containing server address and port
     */
    public NodeInfo findMatchingServer(String keyHashed) {
        for (NodeInfo nodeInfo : meta) {
            if (nodeInfo.getRange().inRange(keyHashed)) {
                return nodeInfo;
            }
        }
        return null;
    }

    /**
     * get metadata loadedDataSize
     *
     * @return metadata loadedDataSize
     */
    public int getLength() {
        return meta.size();
    }

    public List<NodeInfo> get() {
        return meta;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "meta=" + meta +
                '}';
    }
}
