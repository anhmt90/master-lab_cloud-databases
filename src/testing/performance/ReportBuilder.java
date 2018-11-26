package testing.performance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

class ReportBuilder {
  private ArrayList<String> lines = new ArrayList<>();
  private Character delim = ';';
  private boolean firstValue = true;
  private StringBuilder sb = new StringBuilder();

  public void addSb() {
    lines.add(sb.toString());
    sb = new StringBuilder();
    firstValue = true;
  }

  public void addValue(long val) {
    addValue(String.valueOf(val));
  }

  public void addValues(int[] vals) {
    for (int val: vals) {
      addValue(val);
    }
  }

  public void addValues(long[] vals) {
    for (long val: vals) {
      addValue(val);
    }
    addSb();
  }

  public void addValue(String val) {
    if (!firstValue) {
      sb.append(delim);
    }
    sb.append(val);
  }

  public void addHeader(String header) {
    if (sb.length() > 0) {
      addSb();
    }
    lines.add(header);
  }

  public void writeToFile(String path) throws IOException {
    Path p = Paths.get(path);
    writeToFile(p);
  }

  public void writeToFile(Path path) throws IOException {
    addSb();
    Files.write(path, lines);
  }
}
