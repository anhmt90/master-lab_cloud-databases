package mapreduce.server.word_count;

import ecs.KeyHashRange;
import mapreduce.server.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.api.BatchDataTransferProcessor;
import server.app.Server;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.util.*;

public class WordCountMapper extends Mapper<String, Integer> {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);


    public WordCountMapper(String dbPath, KeyHashRange appliedRange) {
        super(dbPath, appliedRange);
        output = new TreeMap<>();
    }

    @Override
    public void map() {
        for (String file : files) {
            try {
                count(FileUtils.getValue(file));
            } catch (IOException e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    private void count(String val) {
        for (String word : val.toLowerCase().replaceAll("[^A-z0-9 ]", StringUtils.EMPTY_STRING).trim().split(" +"))
            output.put(word, output.containsKey(word) ? output.get(word) + 1 : 1);
    }

    public Map<String, Integer> getOutput() {
        return output;
    }
}
