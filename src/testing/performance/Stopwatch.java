package testing.performance;

class Stopwatch {
    private long tick;
    private long tock;
    private long runtime;

    public void tick() {
        tick = System.nanoTime();
    }

    public long tock() {
        tock = System.nanoTime();
        runtime = tock - tick;
        return runtime;
    }

    public double getRuntimeInSeconds() {
        return runtime / 1000000000.0;
    }

    public double getRuntimeInMiliseconds() {
        return runtime / 1000000.0;
    }
}
