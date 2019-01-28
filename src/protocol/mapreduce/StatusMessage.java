package protocol.mapreduce;

import mapreduce.common.TaskType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Message sent from a worker to the collector
 */
public class StatusMessage implements Serializable {
    private TaskType taskType;
    private boolean success;
    private HashSet<String> keySet;

    public StatusMessage(TaskType taskType, boolean success, HashSet<String> keySet) {
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

    public HashSet<String> getKeySet() {
        return keySet;
    }

    @Override
    public String toString() {
        return "StatusMessage{" +
                "taskType=" + taskType +
                ", success=" + success +
                ", keySet=" + Arrays.toString(keySet.toArray()) +
                '}';
    }
}
