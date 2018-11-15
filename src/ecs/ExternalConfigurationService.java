package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.FileUtils;
import util.StringUtils;
import util.Validate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;
import static util.StringUtils.WHITE_SPACE;

public class ExternalConfigurationService {
    public static final String ECS_LOG = "ECS";
    private static Logger LOG = LogManager.getLogger(ECS_LOG);

    private static final String CONFIG_FILE_NAME = "server-info";
    private static final String CONFIG_FILE = WORKING_DIR + SEP + "config" + SEP + CONFIG_FILE_NAME;

    public void initService(int numNodes, int cacheSize, String strategy) {
        String[] endpoints = pickNodes(numNodes);
        for (String endpoint : endpoints) {
            String[] connectionInfo = endpoint.split(WHITE_SPACE);
            Validate.isTrue(connectionInfo.length == 3, "Node " + connectionInfo[0] + " has wrong connection info format");
            String nodeName = connectionInfo[0];
            String address = connectionInfo[1];
            String port = connectionInfo[2];

            String initNodeCmd = "ssh -n " + address + " nohup java -jar " + WORKING_DIR + SEP + "ms3-server.jar "
                    + port + WHITE_SPACE + cacheSize + WHITE_SPACE + strategy + " &";

            Process proc;
            Runtime run = Runtime.getRuntime();
            try {
                proc = run.exec(initNodeCmd);
                proc.waitFor();
                LOG.info(nodeName + "at " + address + "started successfully on port " + port + " (" + cacheSize + ", " + strategy + ")");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    private String[] pickNodes(int numNodes) {
        if (FileUtils.isExisted(CONFIG_FILE)) {
            try {
                List<String> lines = Files.readAllLines(Paths.get(CONFIG_FILE));
                if (lines.size() <= numNodes)
                    return lines.toArray(new String[0]);
                Collections.shuffle(lines);
                lines.subList(0, numNodes - 1).toArray(new String[0]);
            } catch (IOException e) {
                LOG.error(e);
                e.printStackTrace();
            }
        }
        return null;
    }
}
