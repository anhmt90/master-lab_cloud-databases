package testing.performance;

public class Performance {
    /**
     * time spent to execute in second
     */
    private long runtime;
    private long numOps;

    public Performance() {
        this.runtime = 0;
        this.numOps = 0;
    }

    /**
     * calculates how many operations can be executed in a ms
     * @return the average throughput (ops/ms)
     */
    public double getThroughput() {
        return ((double) numOps) / ((double) runtime);
    }

    /**
     * calculates much time an operation need in average
     * @return the average latency (ms/op)
     */
    public double getLatency() {
        return ((double) runtime) / ((double) numOps);
    }

    public long getRuntime() {
        return runtime;
    }

    public long getNumOps() {
        return numOps;
    }

    public Performance withRuntime(long runtime) {
        this.runtime = runtime;
        return this;
    }

    public Performance withNumOps(long numOps) {
        this.numOps = numOps;
        return this;
    }
}
