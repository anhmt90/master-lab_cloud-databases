package testing.performance;

import util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EnronDataset {
  private final Path rootPath;
  private ConcurrentHashMap<String, String> kvMap;
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

    this.kvMap.put(key, value);
  }

  public void loadMessages(int amount) {
    float loadFactor = 0.95f;
    int hmInitSize = (int) (amount * 1.1);
    this.kvMap = new ConcurrentHashMap<>(hmInitSize, loadFactor);

    Collections.shuffle(this.files);

    List<Thread> threads = new ArrayList<>(amount);
    for (int i = 0; i < amount; i++) {
      Path filePath = this.files.get(i);

      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          loadMsg(filePath);
        }
      });
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

  public static void main(String[] args) {
    EnronDataset ed = null;
    try {
      ed = new EnronDataset(FileUtils.WORKING_DIR + "/../../maildir");
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    ed.loadMessages(200);
    System.out.println("Loaded msgs: " + ed.size());
    for (Map.Entry<String, String> entry: ed.loadedEntries().entrySet()) {
      System.out.println(entry.getKey());
      System.out.println(entry.getValue());
    }
  }

  public Map<String, String> loadedEntries() {
    return this.kvMap;
  }

  public int size() {
    return this.kvMap.size();
  }
}
