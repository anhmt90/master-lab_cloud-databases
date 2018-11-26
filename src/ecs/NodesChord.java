package ecs;

import util.HashUtils;
import util.Validate;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Maps server connections in a tree structure
 *
 */
public class NodesChord {
    private TreeMap<String, KVServer> nodes = new TreeMap<>();
    private Metadata md = new Metadata();
    private boolean mdChanged;

    public Optional<KVServer> getSuccessor(String hashKey) {
        Map.Entry<String, KVServer> successor = this.nodes.ceilingEntry(hashKey);
        if (successor == null) {
            successor = this.nodes.firstEntry();
        }
        if (successor != null && successor.getKey().equals(hashKey)) {
            successor = null;
        }
        return successor == null ? Optional.empty() : Optional.ofNullable(successor.getValue());
    }

    public Optional<KVServer> getPredecessor(String hashKey) {
        Map.Entry<String, KVServer> predecessor = this.nodes.lowerEntry(hashKey);
        if (predecessor == null) {
            predecessor = this.nodes.lastEntry();
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
        nodes.put(node.getHashKey(), node);
        this.mdChanged = true;
        return successor;
    }

    public Optional<KVServer> remove(KVServer node) {
        Optional<KVServer> successor = this.getSuccessor(node.getHashKey());
        nodes.remove(node.getHashKey());
        this.mdChanged = true;
        return successor;
    }

    public Metadata getMetadata() {
        if (this.mdChanged) {
            this.md = new Metadata();

            String[] keys = new String[nodes.size()];
            keys = new ArrayList<>(nodes.keySet()).toArray(keys);

            KVServer[] kvServers = new KVServer[nodes.size()];
            kvServers = new ArrayList<>(nodes.values()).toArray(kvServers);


            for (int i = 0; i < nodes.size(); i++){
                String end = keys[i];
                int j  = i - 1 < 0 ? nodes.size() - 1 : i - 1;
                String start = HashUtils.increaseHashBy1(keys[j]);
                KVServer node = kvServers[i];
                md.add(node.getNodeName(), node.getHost(), node.getPort(), start, end);
            }
            this.mdChanged = false;
        }
        return md;
    }

    public Optional<KVServer> randomNode() {
        int n = ThreadLocalRandom.current().nextInt(this.nodes.size());
        for (KVServer kvS : this.nodes()) {
            if (n == 0) {
                return Optional.ofNullable(kvS);
            }
            n--;
        }
        return Optional.empty();
    }

    public Collection<KVServer> nodes() {
        return this.nodes.values();
    }

    /**
     * Checks if Treemap is empty
     *
     * @return true if treemap is empty
     */
    public boolean isEmpty() {
        return nodes.isEmpty();
    }
}
