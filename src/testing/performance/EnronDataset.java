package testing.performance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import testing.AllTests;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnronDataset {
    class KV {

        public final String key;
        public final String val;
        Pattern keyPattern = Pattern.compile("\\d+\\.\\d+");

        public KV(String key, String val) {
            this.val = val;

            Matcher m = keyPattern.matcher(key);
            String s = "stub";
            while (m.find()) {
                s = m.group();
                // s now contains "BAR"
            }
            this.key = s;
        }

    }

    private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

    private static final int MAX_VAL_LENGTH = 122880;
    private final Path datasetPath;
    private CopyOnWriteArrayList<KV> loadedData;
    private List<Path> files = new ArrayList<>();

    public EnronDataset(String datasetPath) throws IOException {
        this.datasetPath = Paths.get(datasetPath);
        if (!Files.exists(this.datasetPath))
            throw new IOException("Enron dataset was not found");
        loadFileLocations();
    }

    private void loadFileLocations() throws IOException {
        Files.walk(this.datasetPath)
                .filter(Files::isRegularFile)
                .forEach(filePath -> files.add(filePath));
        LOG.info(String.format("Number of entries in the dataset: %d", files.size()));
    }


    public void loadData(int amount) {
        this.loadedData = new CopyOnWriteArrayList<>();
        Collections.shuffle(files);

        List<Thread> threads = new ArrayList<>(amount);
        for (int i = 0; i < amount; i++) {
            Path filePath = this.files.get(i);
            Thread t = new Thread(() -> readFile(filePath));
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    private void readFile(Path filePath) {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(filePath);
        } catch (IOException e) {
            LOG.info(String.format("Couldn't read file %s", filePath));
            return;
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = lines.iterator();
        String key = iter.next();
        while (iter.hasNext() && sb.length() <= MAX_VAL_LENGTH)
            sb.append(iter.next());

        String value = sb.toString();
        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            LOG.warn("Key or value is empty. Skipping file " + filePath.toString());
            return;
        }

        this.loadedData.add(new KV(key, value));
    }

    public void loadEntireDataset() {
        this.loadData(this.files.size());
    }

    public CopyOnWriteArrayList<KV> loadedEntries() {
        return this.loadedData;
    }

    public int loadedDataSize() {
        return loadedData.size();
    }

    public KV getRandom() {
        int n = ThreadLocalRandom.current().nextInt(loadedDataSize());
        return loadedData.get(n);
    }
}
