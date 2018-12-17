package management;

import java.io.Serializable;

/**
 * Types of messages regarding failure detection protocol
 */
public enum ReportStatus implements Serializable {
        /**
         * used in heartbeat message to detect failing nodes
         */
        HEARTBEAT,

        /**
         * used in a failure report sent to ECS to report a failing node
         */
        SERVER_FAILED,

        /**
         * used by the ECS to confirm that the faiure report has been received
         */
        REPORT_RECEIVED,
}
