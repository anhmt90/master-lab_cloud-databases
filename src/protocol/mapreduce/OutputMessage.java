package protocol.mapreduce;

import mapreduce.common.TaskType;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Message sent from a worker to the collector
 */
public class OutputMessage implements Serializable {
    private TaskType taskType;
    private HashMap<String, String> outputs;

    public OutputMessage(TaskType taskType, HashMap<String, String> outputs) {
        this.taskType = taskType;
        this.outputs = outputs;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public HashMap<String, String> getOutputs() {
        return outputs;
    }
}
