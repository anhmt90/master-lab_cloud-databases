package mapreduce.common;

import ecs.KeyHashRange;

import java.io.Serializable;
import java.util.HashSet;

public class Task implements Serializable {
    TaskType taskType;
    ApplicationID appId;
    HashSet<String> input;

    KeyHashRange appliedRange;

    public Task(TaskType taskType, ApplicationID appId, HashSet<String> input) {
        this.taskType = taskType;
        this.appId = appId;
        this.input = input;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public ApplicationID getAppId() {
        return appId;
    }

    public HashSet<String> getInput() {
        return input;
    }

    public KeyHashRange getAppliedRange() {
        return appliedRange;
    }

    @Override
    public String toString() {
        return "Task{" + taskType +
                ", " + appId +
                ", input='" + input + '\'' +
                '}';
    }
}
