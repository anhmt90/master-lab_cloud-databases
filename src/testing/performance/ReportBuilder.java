package testing.performance;

import util.FileUtils;
import util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

class ReportBuilder {
    private StringBuilder sb = new StringBuilder();

    public void insert(String line) {
        sb.append(line + "\n");
    }

    public void blankLine() {
        insert(StringUtils.EMPTY_STRING);
    }

    public void lineSeparator() {
        insert("=====================================================================");
    }

    public void save(Path path) throws IOException {
        if (!FileUtils.exists(path)) {
            Files.createFile(path);
        }

        Files.write(path, sb.toString().getBytes());
    }
}
