package mapreduce.server.inverted_index;

import ecs.KeyHashRange;
import mapreduce.server.Mapper;
import util.FileUtils;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

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

                List<String> bestMatches = new ArrayList<>();
                int maxMatchingLength = 0;
                for (String input : getInput()) {
                    int inputWords = input.split("\\s+").length;
                    if (val.contains(input)) {
                        if (inputWords >= maxMatchingLength) {
                            bestMatches.add(input);
                            maxMatchingLength = inputWords;
                            bestMatches.removeIf(bm -> bm.split("\\s+").length < inputWords);
                        }
                    }
                }
                for (String match : bestMatches) {
                    output.put(match, output.containsKey(match) ? output.get(match) + "\n" + key : key);
                }
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