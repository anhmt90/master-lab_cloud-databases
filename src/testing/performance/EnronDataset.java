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

import static util.FileUtils.USER_DIR;

public class EnronDataset {
    class KV {

        public final String key;
        public final String val;
        Pattern keyPattern = Pattern.compile("\\d+\\.\\d+");

        public KV(String key, String val) {
            this.val = val;

            Matcher m = keyPattern.matcher(key);
            String s = "";
            while (m.find()) {
                s = m.group();
            }
            this.key = s;
        }

    }

    private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

    private static final int MAX_VAL_LENGTH = 122880;
//    private static final String ENRON_DATASET = USER_DIR + "/../enron_mail_20150507/maildir";
    private static final String ENRON_DATASET = USER_DIR + "/../maildir";

    private final Path datasetPath;
    private ArrayList<KV> dataLoaded;
    private List<Path> files = new ArrayList<>();

    public EnronDataset() throws IOException {
        this.datasetPath = Paths.get(ENRON_DATASET);
        if (!FileUtils.dirExists(this.datasetPath))
            throw new IOException("Enron dataset was not found");
        loadFileLocations();
    }

    private void loadFileLocations() throws IOException {
        Files.walk(this.datasetPath)
                .filter(FileUtils::isFile)
                .forEach(filePath -> files.add(filePath));
        LOG.info(String.format("Number of entries in the dataset: %d", files.size()));
    }


    public void loadData(int amount) {
        this.dataLoaded = new ArrayList<>(amount * 2);
        Collections.shuffle(files);

        for (int i = 0; i < amount; i++) {
            Path filePath = this.files.get(i);
            readFile(filePath);
        }

        LOG.info("Numbers of data loaded " + dataLoaded.size());
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

        if (sb.length() > MAX_VAL_LENGTH) {
            System.out.println("VALUE too large!");
            return;
        }
        String value = sb.toString();
        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
            LOG.warn("Key or value is empty. Skipping file " + filePath.toString());
            return;
        }

        this.dataLoaded.add(new KV(key, value));
    }

    public void loadEntireDataset() {
        this.loadData(this.files.size());
    }

    public ArrayList<KV> getDataLoaded() {
        return this.dataLoaded;
    }

    public int loadedDataSize() {
        return dataLoaded.size();
    }

    public KV getRandom() {
        int n = ThreadLocalRandom.current().nextInt(loadedDataSize());
        return dataLoaded.get(n);
    }
}
