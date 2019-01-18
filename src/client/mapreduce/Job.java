package client.mapreduce;

import mapreduce.common.ApplicationID;
import mapreduce.common.TaskType;

import java.time.Instant;
import java.util.HashSet;

public class Job {
    private Step step;
    private String token;
    private final ApplicationID applicationID;
    HashSet<String> input;

    public Job(ApplicationID applicationID, HashSet<String> input) {
        step = Step.MAP;
        token = String.valueOf(System.currentTimeMillis());
        this.applicationID = applicationID;
        this.input = input;
    }

    public Step getStep() {
        return step;
    }

    public void setStep(Step step) {
        this.step = step;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
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
}
