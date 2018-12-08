package ecs;

import util.HashUtils;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Maps server connections in a tree structure
 *
 */
public class NodesChord {
    private TreeMap<String, KVServer> nodesMap = new TreeMap<>();
    private Metadata md = new Metadata();
    private boolean mdChanged;

    public Optional<KVServer> getSuccessor(String hashKey) {
        Map.Entry<String, KVServer> successor = this.nodesMap.ceilingEntry(hashKey);
        if (successor == null) {
            successor = this.nodesMap.firstEntry();
        }
        if (successor != null && successor.getKey().equals(hashKey)) {
            successor = null;
        }
        return successor == null ? Optional.empty() : Optional.ofNullable(successor.getValue());
    }

    public Optional<KVServer> getPredecessor(String hashKey) {
        Map.Entry<String, KVServer> predecessor = this.nodesMap.lowerEntry(hashKey);
        if (predecessor == null) {
            predecessor = this.nodesMap.lastEntry();
        }
        if (predecessor != null && predecessor.getKey().equals(hashKey)) {
            predecessor = null;
        }
        return predecessor == null ? Optional.empty() : Optional.ofNullable(predecessor.getValue());
    }


    public Optional<KVServer> add(KVServer node) {
        Optional<KVServer> successor = this.getSuccessor(node.getHashKey());

        successor.ifPresent(kvS -> {
            if (kvS.getHashKey().equals(node.getHashKey())) {
                throw new IllegalArgumentException("Node is already in the chord");
            }
        });
        nodesMap.put(node.getHashKey(), node);
        this.mdChanged = true;
        return successor;
    }

    public Optional<KVServer> remove(KVServer node) {
        Optional<KVServer> successor = getSuccessor(node.getHashKey());
        nodesMap.remove(node.getHashKey());
        this.mdChanged = true;
        return successor;
    }

    public Metadata getMetadata() {
        if (this.mdChanged) {
            this.md = new Metadata();

            String[] keys = new String[nodesMap.size()];
            keys = new ArrayList<>(nodesMap.keySet()).toArray(keys);

            KVServer[] kvServers = new KVServer[nodesMap.size()];
            kvServers = new ArrayList<>(nodesMap.values()).toArray(kvServers);

            for (int i = 0; i < nodesMap.size(); i++){
                String end = keys[i];
                int j  = i - 1 < 0 ? nodesMap.size() - 1 : i - 1;
                String start = HashUtils.increaseHashBy1(keys[j]);
                KVServer node = kvServers[i];
                md.add(node.getNodeName(), node.getHost(), node.getServicePort(), start, end);
            }
            this.mdChanged = false;
        }
        return md;
    }

    public Optional<KVServer> randomNode() {
        int n = ThreadLocalRandom.current().nextInt(this.nodesMap.size());
        for (KVServer kvS : this.nodes()) {
            if (n == 0) {
                return Optional.ofNullable(kvS);
            }
            n--;
        }
        return Optional.empty();
    }

    public List<KVServer> nodes() {
        return this.nodesMap.values().stream().sequential().collect(Collectors.toList());
    }

    public int size() {
        return nodesMap.size();
    }

    /**
     * Checks if Treemap is empty
     *
     * @return true if treemap is empty
     */
    public boolean isEmpty() {
        return nodesMap.isEmpty();
    }

    public TreeMap<String, KVServer> getNodesMap() {
        return nodesMap;
    }
}
