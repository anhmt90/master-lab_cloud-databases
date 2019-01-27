package client.mapreduce;

import mapreduce.common.ApplicationID;
import protocol.mapreduce.Utils;
import util.StringUtils;

import java.util.TreeSet;

import static protocol.mapreduce.Utils.WITHIN_JOBID_SEP;

public class Job {
    private Step step;
    private String jobId;
    private final ApplicationID applicationID;
    private TreeSet<String> input;

    public Job(ApplicationID applicationID, TreeSet<String> input) {
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

    public TreeSet<String> getInput() {
        return input;
    }

    public void setInput(TreeSet<String> input) {
        this.input = input;
    }

    public String setJobIdBeforeMap() {
        jobId = applicationID.getId() + WITHIN_JOBID_SEP + jobId +  StringUtils.getRandomString();
        return jobId;
    }

    public String setJobIdAfterMap() {
        jobId = Utils.updateJobIdAfterMap(jobId);
        return jobId;
    }

    public String setJobIdAfterReduce() {
        jobId = Utils.updateJobIdAfterReduce(jobId);
        return jobId;
    }
}
