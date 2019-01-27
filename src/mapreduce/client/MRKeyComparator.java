package mapreduce.client;

import ecs.Metadata;
import ecs.NodeInfo;
import util.HashUtils;

import java.util.Comparator;

public class MRKeyComparator implements Comparator<String> {
    private Metadata metadata;

    public MRKeyComparator(Metadata metadata) {
        this.metadata = metadata;
    }


    @Override
    public int compare(String a, String b) {
        String hashedA = HashUtils.hash(a);
        String hashedB = HashUtils.hash(b);

        NodeInfo nodeA = metadata.getCoordinator(hashedA);
        NodeInfo nodeB = metadata.getCoordinator(hashedB);

        return nodeA.getId().compareTo(nodeB.getId());
    }
}
