package mapreduce.server;

import client.mapreduce.Driver;
import ecs.KeyHashRange;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.api.BatchDataTransferProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class Mapper {
    private static Logger LOG = LogManager.getLogger(Driver.MAPREDUCE_LOG);
    protected ArrayList<String> files;

    public abstract void map() throws IOException;

    public Mapper(String dbPath, KeyHashRange appliedRange) {
        collectAppliedFiles(new BatchDataTransferProcessor(dbPath).indexData(appliedRange));
    }

    public ArrayList<String> getFiles() {
        return files;
    }

    private void collectAppliedFiles(String[] indexFiles) {
        for (String indexFile : indexFiles) {
            List<String> appliedFiles = null;
            try {
                appliedFiles = Files.readAllLines(Paths.get(indexFile));
            } catch (IOException e) {
                LOG.error(e);
            }
            if (appliedFiles == null)
                continue;
            files.addAll(appliedFiles);
        }
        LOG.info("Number of files need to be processed: " + files.size());
    }
}
