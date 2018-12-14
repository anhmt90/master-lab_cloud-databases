package management;

import ecs.NodeInfo;

import java.io.Serializable;

public class FailureReportMessage implements Serializable {
    private FailureStatus status;
    private NodeInfo failedServer;

    public FailureReportMessage(FailureStatus status) {
        this.status = status;
    }

    public FailureStatus getStatus() {
        return status;
    }
    public FailureReportMessage(FailureStatus status, NodeInfo target) {
        this.status = status;
        this.failedServer = target;
    }

    public NodeInfo getTargetServer() {
        return failedServer;
    }

    @Override
    public String toString() {
        return "FailureReport{" +"status=" + status +'}';
    }
}
