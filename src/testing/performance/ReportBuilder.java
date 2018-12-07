package testing.performance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

class ReportBuilder {
    private ArrayList<String> lines = new ArrayList<>();
    private static final char DELIM = ';';
    private boolean atLineStart = true;
    private StringBuilder sb = new StringBuilder();

    public void startNewLine() {
        lines.add(sb.toString());
        sb = new StringBuilder();
        atLineStart = true;
    }

    public void addNewLineWith(Integer[] vals) {
        for (int val : vals) {
            appendToLine(val);
        }
        startNewLine();
    }

    public void addNewLineWith(Long[] vals) {
        for (long val : vals)
            appendToLine(val);
        startNewLine();
    }

    public void addNewLineWith(Double[] vals) {
        for (double val : vals)
            appendToLine(val);
        startNewLine();
    }

    public void appendToLine(long val) {
        appendToLine(String.valueOf(val));
    }

    public void appendToLine(double val) {
        appendToLine(String.valueOf(val));
    }

    public void appendToLine(String val) {
        if (!atLineStart)
            sb.append(DELIM);

        sb.append(val);
        atLineStart = false;
    }


    public void addHeader(String header) {
        if (sb.length() > 0) {
            startNewLine();
        }
        lines.add(header);
    }

    public void writeToFile(String path) throws IOException {
        startNewLine();
        Path p = Paths.get(path);
        if (!Files.exists(p)) {
            File file = new File(path);
            file.createNewFile();
        }
        Files.write(p, lines);
    }

    public void writeToFile(Path path) throws IOException {
        writeToFile(path.toString());
    }
}
