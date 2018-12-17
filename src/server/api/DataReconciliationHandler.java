package server.api;

import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;
import util.Validate;

public class DataReconciliationHandler {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private final Server server;
    private Metadata oldMetadata;
    private KeyHashRange oldWriteRange;
    private KeyHashRange oldReadRange;
    private KeyHashRange oldWriteRangeOfReplica2;

    public DataReconciliationHandler(Server server) {
        this.server = server;
    }

    public boolean reconcile() {
        if (oldMetadata != null)
            try {
                if (oldMetadata.getLength() < server.getMetadata().getLength())
                    handleScaleUp();
                else if (oldMetadata.getLength() > server.getMetadata().getLength())
                    handleScaleDown();
                return true;
            } catch (RuntimeException e) {
                LOG.error("Runtime Exception", e);
                return false;
            }
        return true;
    }

    private void handleScaleDown() {
        KeyHashRange currentWriteRangeOfRep2 = server.getReplicator2().getReplica().getWriteRange();
        if (oldWriteRangeOfReplica2.isSubRangeOf(currentWriteRangeOfRep2)) {
            boolean done = server.lockWrite();
            Validate.isTrue(done, "lock write on this node failed!");

            KeyHashRange transferred = server.getWriteRange();
            done = server.moveData(transferred, server.getReplicator2().getReplica());
            Validate.isTrue(done, "move data failed!");

            done = server.unlockWrite();
            Validate.isTrue(done, "unlock write on this node failed!");
        }
    }

    private void handleScaleUp() {
        if (server.getWriteRange().isSubRangeOf(oldWriteRange)) {
            boolean done = server.lockWrite();
            Validate.isTrue(done, "lock write on node " + server + " failed!");

            NodeInfo predecessor = server.getMetadata().getPredecessor(server.getWriteRange());
            //TODO improve performance: transfer the writeRange -> unlock write -> transfer (readRange - writeRange)
            KeyHashRange transferred = new KeyHashRange(oldReadRange.getStart(), predecessor.getWriteRange().getEnd());
            done = server.moveData(transferred, predecessor);
            Validate.isTrue(done, "move data failed!");

            done = server.unlockWrite();
            Validate.isTrue(done, "unlock write on this node failed!");
        }
    }

    public DataReconciliationHandler withOldMetadata(Metadata oldMetadata) {
        this.oldMetadata = oldMetadata;
        return this;
    }

    public DataReconciliationHandler withOldWriteRange(KeyHashRange oldWriteRange) {
        this.oldWriteRange = oldWriteRange;
        return this;
    }

    public DataReconciliationHandler withOldReadRange(KeyHashRange oldReadRange) {
        this.oldReadRange = oldReadRange;
        return this;
    }

    public DataReconciliationHandler withOldWriteRangeOfReplica2(KeyHashRange oldWriteRangeOfRep2) {
        this.oldWriteRangeOfReplica2 = oldWriteRangeOfRep2;
        return this;
    }
}
