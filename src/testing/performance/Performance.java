package testing.performance;

public class Performance {
    /**
     * time spent to execute in millisecond
     */
    private long runtime;
    private long numOps;

    public Performance() {
        this.runtime = 0;
        this.numOps = 0;
    }

    /**
     * calculates the average throughput
     * @return the average throughput (ops/ms)
     */
    public double getThroughput() {
        return numOps / runtime;
    }

    /**
     * calculates the average latency per operation
     * @return the average latency (ms/op)
     */
    public double getLatency() {
        return runtime / numOps;
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
