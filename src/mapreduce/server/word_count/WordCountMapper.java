package mapreduce.server.word_count;

import ecs.KeyHashRange;
import mapreduce.server.Mapper;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class WordCountMapper extends Mapper<String, Integer> {
    public WordCountMapper(String dbPath, KeyHashRange appliedRange) {
        super(dbPath, appliedRange);
    }

    @Override
    public void map() {
        for (String file : files) {
            try {
                count(FileUtils.getValue(file));

            } catch (IOException e) {
                LOG.error(e);
            } catch (RuntimeException e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
        LOG.info("Finish mapping: " + Arrays.toString(output.keySet().toArray()));
    }

    private void count(String text) {
        for (String word : text.toLowerCase().replaceAll("[^A-z0-9 ]", StringUtils.EMPTY_STRING).trim().split(" +"))
            output.put(word, output.containsKey(word) ? output.get(word) + 1 : 1);
    }


    public Map<String, Integer> getOutput() {
        return output;
    }
}
