package mapreduce.common;

import client.mapreduce.Job;
import ecs.KeyHashRange;

import java.io.Serializable;
import java.util.HashSet;

public class Task implements Serializable {
    private TaskType taskType;
    private ApplicationID appId;
    private HashSet<String> input;
    private String jobId;

    KeyHashRange appliedRange;

    public Task(TaskType taskType, Job job) {
        this.taskType = taskType;
        this.appId = job.getApplicationID();
        this.input = job.getInput();
        this.jobId = job.finalizeJobId();

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

    public String getJobId() {
        return jobId;
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
