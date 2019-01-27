package protocol.mapreduce;

import util.Validate;

public class Utils {
    private static final char INTERMEDIATE_OUTPUT = 'i';
    private static final char FINAL_OUTPUT = 'f';
    public static final String WITHIN_JOBID_SEP = "_";
    public static final String JOBID_NODEID_SEP = ".";
    public static final String NODEID_KEYBYTES_SEP = JOBID_NODEID_SEP;

    public static String updateJobIdAfterMap(String jobId) {
        Validate.isTrue(jobId.indexOf(WITHIN_JOBID_SEP) > 0, "Invalid jobId format");
        String[] components = jobId.split(WITHIN_JOBID_SEP);
        Validate.isTrue(components[0].length() == 2, "Invalid appId format");

        String appId = INTERMEDIATE_OUTPUT + components[0];
        String rest = components[1];
        return appId + WITHIN_JOBID_SEP + rest;
    }

    public static String updateJobIdAfterReduce(String jobId) {
        Validate.isTrue(jobId.indexOf(WITHIN_JOBID_SEP) > 0 && jobId.charAt(0) == INTERMEDIATE_OUTPUT, "Invalid jobId format");
        String[] components = jobId.split(WITHIN_JOBID_SEP);

        Validate.isTrue(components[0].length() == 3, "Invalid appId format");
        String appId = FINAL_OUTPUT + components[0].substring(1,3);
        String rest = components[1];
        return appId + WITHIN_JOBID_SEP + rest;
    }
}
