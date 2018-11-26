package testing.performance;

class Stopwatch {
  private long tick = System.nanoTime();

  long tick() {
    long tock = System.nanoTime();
    long elapsed = tock - tick;
    this.tick = tock;
    return elapsed;
  }

}
