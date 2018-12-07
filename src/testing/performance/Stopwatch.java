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

    public long getRuntimeInSeconds() {
        return runtime / 1_000_000_000;
    }

    public long getRuntimeInMiliseconds() {
        return runtime / 1_000_000;
    }
}
