package mapreduce.server.word_count;

import ecs.KeyHashRange;
import mapreduce.server.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;
import util.FileUtils;

import java.io.IOException;
import java.util.Set;

import static util.FileUtils.getKeyFromStringPath;

public class WordCountReducer extends Reducer<String, Integer> {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    public WordCountReducer(String dbPath, KeyHashRange appliedRange, String prefix) {
        super(dbPath, appliedRange, prefix);
    }

    @Override
    public void reduce() {
        for (String file : files) {
            String key = getKeyFromStringPath(file);
            try {
                accumulate(key, FileUtils.getValue(file));
            } catch (IOException e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Set<String> getKeySet() {
        return output.keySet();
    }

    private void accumulate(String key, String value) {
        output.put(key, output.containsKey(key) ? output.get(key) + Integer.valueOf(value) : Integer.valueOf(value));
    }
}
