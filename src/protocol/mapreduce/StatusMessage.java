package protocol.mapreduce;

import mapreduce.common.TaskType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Message sent from a worker to the collector
 */
public class StatusMessage<KT> implements Serializable {
    private TaskType taskType;
    private boolean success;
    private Set<String> keySet;

    public StatusMessage(TaskType taskType, boolean success, Set<String> keySet) {
        this.taskType = taskType;
        this.success = success;
        this.keySet = keySet;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public boolean isSuccess() {
        return success;
    }

    public Set<String> getKeySet() {
        return keySet;
    }
}
