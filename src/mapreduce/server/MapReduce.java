package mapreduce.server;

import client.mapreduce.Driver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.Utils;
import server.app.Server;
import util.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class MapReduce<KT, VT> {
    protected static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    protected Map<KT, VT> output;
    protected ArrayList<String> files;

    protected String prefix;

    public Map<KT, VT> getOutput() {
        return output;
    }

    public ArrayList<String> getFiles() {
        return files;
    }

    public MapReduce(String prefix) {
        this.prefix = prefix;
        if (prefix.equals(StringUtils.EMPTY_STRING))
            this.prefix += Utils.NODEID_KEYBYTES_SEP;
    }

    protected void collectFiles(String[] indexFiles) {
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
        LOG.info("Number of files to process: " + files.size());
    }


}
