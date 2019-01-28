package mapreduce.server.inverted_index;

import ecs.KeyHashRange;
import mapreduce.server.Reducer;
import util.FileUtils;

import java.io.IOException;
import java.util.Arrays;

import static util.FileUtils.getKeyFromStringPath;

public class InvertedIndexReducer extends Reducer<String, String> {
    public InvertedIndexReducer(String dbPath, KeyHashRange appliedRange, String prefix) {
        super(dbPath, appliedRange, prefix);
    }

    @Override
    public void reduce() {
        LOG.info("Reduced files: " + Arrays.toString(files.toArray()));
        for (String file : files) {
            String key = getKeyFromStringPath(file);
            LOG.info("key: " + key);
            try {
                String val = FileUtils.getValue(file);
                LOG.info("val: " + val);
                accumulate(key, val);
            } catch (IOException e) {
                LOG.error(e);
            } catch (RuntimeException e) {
                LOG.error("Exception: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    private void accumulate(String key, String value) {
        output.put(key, output.containsKey(key) ? output.get(key) + "\n" + value : value);
    }
}
