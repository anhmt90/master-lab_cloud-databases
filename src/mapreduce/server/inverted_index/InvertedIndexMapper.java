package mapreduce.server.inverted_index;

import ecs.KeyHashRange;
import mapreduce.server.Mapper;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

import static util.FileUtils.getKeyFromStringPath;

public class InvertedIndexMapper extends Mapper<String, String> {

    public InvertedIndexMapper(String dbPath, KeyHashRange appliedRange, TreeSet<String> inputs) {
        super(dbPath, appliedRange);
        setInput(inputs);
    }

    @Override
    public void map() {
        for (String file : files) {
            LOG.info("file: " + file);
            try {
                String key = StringUtils.decode(Paths.get(file).getFileName().toString());
                LOG.info("key: " + key);

                String val = FileUtils.getValue(file);
                LOG.info("val: " + val);

                String bestMatch = StringUtils.EMPTY_STRING;
                for(String input : getInput()) {
                    if(val.contains(input)) {
                        if(input.length() > bestMatch.length())
                            bestMatch = input;
                    }
                }

                output.put(bestMatch, output.containsKey(bestMatch) ? output.get(bestMatch) + "\n" + key: key);
            } catch (IOException e) {
                LOG.error(e);
            } catch (RuntimeException e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
        LOG.info("Finish mapping: " + Arrays.toString(output.keySet().toArray()));
    }

}