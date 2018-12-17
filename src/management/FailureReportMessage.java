package management;

import ecs.NodeInfo;

import java.io.Serializable;

/**
 * Failure Report exchanged between ECS and storage servers when a failure is detected in the service
 * 
 */
public class FailureReportMessage implements Serializable {
    private ReportStatus status;
    private NodeInfo failedServer;

    public FailureReportMessage(ReportStatus status) {
        this.status = status;
    }

    public FailureReportMessage(ReportStatus status, NodeInfo target) {
        this.status = status;
        this.failedServer = target;
    }

    public NodeInfo getFailedServer() {
        return failedServer;
    }

    public ReportStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "FailureReport{" +"status=" + status +'}';
    }
}
