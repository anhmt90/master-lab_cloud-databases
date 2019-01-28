package mapreduce.common;

import mapreduce.client.Job;
import ecs.KeyHashRange;

import java.io.Serializable;
import java.util.TreeSet;

public class Task implements Serializable {
    private TaskType taskType;
    private ApplicationID appId;
    private TreeSet<String> input;
    private String jobId;

    KeyHashRange appliedRange;

    public Task(TaskType taskType, Job job) {
        this.taskType = taskType;
        this.appId = job.getApplicationID();
        this.input = job.getInput();
        this.jobId = job.getJobId();
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public ApplicationID getAppId() {
        return appId;
    }

    public TreeSet<String> getInput() {
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
