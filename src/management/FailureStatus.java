package management;

import java.io.Serializable;

public enum FailureStatus implements Serializable {
        HEARTBEAT,
        SERVER_FAILURE,
        FAILURE_RESOLVED,
        FAILURE_UNRESOLVED
}
