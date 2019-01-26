package client.mapreduce;

import mapreduce.common.ApplicationID;
import util.StringUtils;

import java.util.HashSet;

public class Job {
    private Step step;
    private String jobId;
    private final ApplicationID applicationID;
    HashSet<String> input;

    public Job(ApplicationID applicationID, HashSet<String> input) {
        step = Step.MAP;
        jobId = String.valueOf(System.nanoTime());
        this.applicationID = applicationID;
        this.input = input;
    }

    public Step getStep() {
        return step;
    }

    public void setStep(Step step) {
        this.step = step;
    }

    public String getJobId() {
        return jobId;
    }

    public ApplicationID getApplicationID() {
        return applicationID;
    }

    public HashSet<String> getInput() {
        return input;
    }

    public void setInput(HashSet<String> input) {
        this.input = input;
    }

    public String finalizeJobId() {
        return jobId += StringUtils.getRandomString();

    }
}
