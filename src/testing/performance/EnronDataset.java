package testing.performance;

import util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class EnronDataset {
  class KV {
    public final String key;
    public final String val;

    public KV(String key, String val) {
      this.key = key;
      this.val = val;
    }

  }
  private final Path rootPath;
  private CopyOnWriteArrayList<KV> kvs;
  private List<Path> files = new ArrayList<Path>();

  public EnronDataset(String datasetPath) throws IOException {
    this.rootPath = Paths.get(datasetPath);
    if (!Files.exists(this.rootPath)) {
      throw new IOException("Enron dataset was not found");
    }
    loadFileNames();
  }

  private void loadFileNames() throws IOException {
    Files.walk(this.rootPath)
        .filter(Files::isRegularFile)
        .forEach(filePath -> this.files.add(filePath));
    System.out.println(String.format("Number of entries in the dataset: %d", this.files.size()));
  }

  private void loadMsg(Path filePath) {
    List<String> lines = null;
    try {
      lines = Files.readAllLines(filePath);
    } catch (IOException e) {
      System.out.println(String.format("Couldn't read a file: %s", filePath));
      return;
    }

    StringBuilder sb = new StringBuilder();
    Iterator<String> iter = lines.iterator();
    String key = iter.next();
    while (iter.hasNext()) {
      sb.append(iter.next());
    }
    String value = sb.toString();

    this.kvs.add(new KV(key, value));
  }

  public void loadMessages(int amount) {
    this.kvs = new CopyOnWriteArrayList<>();

    Collections.shuffle(this.files);

    List<Thread> threads = new ArrayList<>(amount);
    for (int i = 0; i < amount; i++) {
      Path filePath = this.files.get(i);

      Thread t = new Thread(() -> loadMsg(filePath));
      t.start();
      threads.add(t);
    }

    for (Thread t: threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void loadAllMessages() {
    this.loadMessages(this.files.size());
  }

  public static void main(String[] args) {
    EnronDataset ed = null;
    try {
      ed = new EnronDataset(FileUtils.WORKING_DIR + "/../../maildir");
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    ed.loadMessages(2);
    System.out.println("Loaded msgs: " + ed.size());
  }

  public CopyOnWriteArrayList<KV> loadedEntries() {
    return this.kvs;
  }

  public int size() {
    return this.kvs.size();
  }

  public KV getRandom() {
    int n = ThreadLocalRandom.current().nextInt(this.size());
    return this.kvs.get(n);
  }
}
