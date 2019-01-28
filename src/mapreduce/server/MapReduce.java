package mapreduce.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.Utils;
import server.app.Server;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public abstract class MapReduce<KT, VT> {
    protected static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    protected Map<KT, VT> output;
    protected Set<KT> input;
    protected ArrayList<String> files;

    protected String prefix;

    public MapReduce(String prefix) {
        files = new ArrayList<>();
        output = new TreeMap<>();
        input = new TreeSet<>();
        this.prefix = prefix;
        if (prefix.equals(StringUtils.EMPTY_STRING))
            this.prefix += Utils.NODEID_KEYBYTES_SEP;
    }

    public HashSet<KT> getKeySet() {
        return new HashSet<>(output.keySet());
    }

    public Map<KT, VT> getOutput() {
        return output;
    }

    public ArrayList<String> getFiles() {
        return files;
    }

    public void setInput(Set<KT> input) {
        this.input = input;
    }

    public Set<KT> getInput() {
        return input;
    }

    protected void collectFiles(String[] indexFiles) {
        for (String indexFile : indexFiles) {
            List<String> appliedFiles = null;
            try {
                LOG.debug("Reading all lines from index file: " + indexFile);
                appliedFiles = Files.readAllLines(Paths.get(indexFile));
            } catch (IOException e) {
                LOG.error(e);
            }
            if (appliedFiles == null)
                continue;
            LOG.debug("Adding files to collection: " + Arrays.toString(appliedFiles.toArray()));
            files.addAll(appliedFiles);
        }
        LOG.info("Number of files to process: " + files.size());
    }


}
