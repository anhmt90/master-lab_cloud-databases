package mapreduce.server;

import ecs.KeyHashRange;
import server.api.BatchDataTransferProcessor;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.util.*;

public class WordCountMapper extends Mapper {
    Map<String, Integer> output;

    public WordCountMapper(String dbPath, KeyHashRange appliedRange) {
        super(dbPath, appliedRange);
        output = new TreeMap<String, Integer>();
    }

    @Override
    public void map() throws IOException {
        for (String file : files)
            count(FileUtils.getValue(file));
    }

    private void count(String val) {
        for (String word : val.toLowerCase().replaceAll("[^a-z ]", StringUtils.EMPTY_STRING).trim().split(" +"))
            output.put(word, output.containsKey(word) ? output.get(word) + 1 : 1);
    }

    public Map<String, Integer> getOutput() {
        return output;
    }
}
