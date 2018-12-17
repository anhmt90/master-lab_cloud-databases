package management;

import java.io.Serializable;

public enum ReportStatus implements Serializable {
        HEARTBEAT,
        SERVER_FAILED,
        REPORT_RECEIVED,
}
