package server.api;

import ecs.KeyHashRange;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.*;
import server.app.Server;
import util.HashUtils;
import util.LogUtils;
import util.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static util.FileUtils.SEP;

public class BatchDataTransferProcessor {
    private static final String DATA_TRANSFER_INDEX_FOLDER = System.getProperty("user.dir") + SEP + "dti" + SEP;
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    /**
     * The socket being used to move data when adding/removing servers
     */
    private Socket moveDataSocket;
    BufferedOutputStream bos;
    BufferedInputStream bis;

    NodeInfo target;
    String dbPath;

    public BatchDataTransferProcessor(NodeInfo target, String dbPath) {
        this.target = target;
        this.dbPath = dbPath;
    }

    private void initSocket() throws IOException {
        connect();
        bos = new BufferedOutputStream(moveDataSocket.getOutputStream());
        bis = new BufferedInputStream(moveDataSocket.getInputStream());
    }

    public boolean handleTransferData(KeyHashRange range) {
        String[] indexFiles = new String[0];
        try {
            indexFiles = indexRelevantDataFiles(range);
            return transfer(indexFiles);
        } catch (IOException ioe) {
            return LogUtils.exitWithError(LOG, ioe);
        } finally {
            try {
                cleanUp(indexFiles);
            } catch (IOException ioe) {
                return LogUtils.exitWithError(LOG, ioe);
            }
        }
    }

    private void cleanUp(String[] indexFiles) throws IOException {
        if(indexFiles.length > 0) {
            for (String indexFile : indexFiles) {
                Path indexFilePath = Paths.get(indexFile);
                for (String dataFile: Files.readAllLines(indexFilePath)) {
                    Files.deleteIfExists(Paths.get(dataFile));
                }
                Files.deleteIfExists(Paths.get(indexFile));
            }
        }

        Path[] indexFilesToRemove = Files.list(Paths.get(DATA_TRANSFER_INDEX_FOLDER))
                .filter(Files::isRegularFile)
                .toArray(Path[]::new);
        for (Path p : indexFilesToRemove)
            Files.deleteIfExists(p);
        Files.deleteIfExists(Paths.get(DATA_TRANSFER_INDEX_FOLDER));
    }

    public String[] indexRelevantDataFiles(KeyHashRange range) {
        if (range.isWrappedAround()) {
            KeyHashRange leftRange = new KeyHashRange(range.getStart(), new String(new char[8]).replace("\0", "ffff"));
            KeyHashRange rightRange = new KeyHashRange(new String(new char[8]).replace("\0", "0000"), range.getEnd());
            return Stream.concat(Arrays.stream(indexRelevantDataFiles(leftRange)), Arrays.stream(indexRelevantDataFiles(rightRange)))
                    .toArray(String[]::new);
        }
        String[] start = StringUtils.splitEvery(range.getStart(), 2);
        String[] end = StringUtils.splitEvery(range.getEnd(), 2);
        String commonPrefix = StringUtils.getLongestCommonPrefix(range.getStart(), range.getEnd());

        String startDir = start[commonPrefix.length() / 2];
        String endDir = end[commonPrefix.length() / 2];

        String commonParent = commonPrefix.length() == 0 ?
                StringUtils.EMPTY_STRING :
                StringUtils.joinSeparated(Arrays.copyOfRange(start, 0, commonPrefix.length() / 2 - 1), SEP);

        String[] firstDiffDirs = new String[0];
        try {
            firstDiffDirs = getSortedChildDirs(commonParent);
            int lowerBound = getIndex(startDir, firstDiffDirs);
            int upperBound = getIndex(endDir, firstDiffDirs);

            int from = lowerBound < 0 ? -(lowerBound + 1) : lowerBound + 1;
            int to = upperBound < 0 ? -(upperBound + 1) : upperBound;

            String[] middleFullRangeDirs = Arrays.copyOfRange(firstDiffDirs, from, to);
            List<String> indexFiles = Arrays.stream(getFilesRecursively(commonParent, middleFullRangeDirs)).collect(Collectors.toList());

            walkStart(start, commonPrefix, firstDiffDirs, lowerBound, indexFiles);
            walkEnd(end, commonPrefix, firstDiffDirs, upperBound, indexFiles);
            return indexFiles.toArray(new String[indexFiles.size()]);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    private void walkStart(String[] start,
                           String commonPrefix,
                           String[] firstDiffDirs,
                           int lowerBound,
                           List<String> indexFiles) throws IOException {
        String currDir = StringUtils.EMPTY_STRING;
        int i = commonPrefix.length() / 2 + 1;
        String[] directChildren = Arrays.copyOfRange(firstDiffDirs, 0, firstDiffDirs.length);
        while (lowerBound >= 0) {
            currDir = currDir + directChildren[lowerBound] + SEP;
            directChildren = getSortedChildDirs(currDir);
            if (directChildren.length == 0) {
                visitLastFile(indexFiles, currDir);
                break;
            }
            lowerBound = getIndex(start[i], directChildren);
            int from = lowerBound < 0 ? -(lowerBound + 1) : lowerBound + 1;
            String[] fullRangeDirs = Arrays.copyOfRange(directChildren, from, directChildren.length);
            String[] startIndexFiles = getFilesRecursively(currDir, fullRangeDirs);
            indexFiles.addAll(Arrays.asList(startIndexFiles));
            i++;
        }
    }

    private void walkEnd(String[] end,
                         String commonPrefix,
                         String[] firstDiffDirs,
                         int upperBound,
                         List<String> indexFiles) throws IOException {
        String currDir = StringUtils.EMPTY_STRING;
        int i = commonPrefix.length() / 2 + 1;
        String[] directChildren = Arrays.copyOfRange(firstDiffDirs, 0, firstDiffDirs.length);
        while (upperBound >= 0) {
            currDir = currDir + directChildren[upperBound] + SEP;
            directChildren = getSortedChildDirs(currDir);
            if (directChildren.length == 0) {
                visitLastFile(indexFiles, currDir);
                break;
            }
            upperBound = getIndex(end[i], directChildren);
            int to = upperBound < 0 ? -(upperBound + 1) : upperBound;
            String[] fullRangeDirs = Arrays.copyOfRange(directChildren, 0, to);
            String[] endIndexFiles = getFilesRecursively(currDir, fullRangeDirs);
            indexFiles.addAll(Arrays.asList(endIndexFiles));
            i++;
        }
    }

    private void visitLastFile(List<String> indexFiles, String currDir) throws IOException {
        String name = StringUtils.removeChar(currDir, SEP.charAt(0));
        Path indexFile = Paths.get(DATA_TRANSFER_INDEX_FOLDER + name);
        Files.deleteIfExists(indexFile);
        Files.createFile(indexFile);
        indexFile = Files.write(indexFile, (dbPath + currDir + name).getBytes(), StandardOpenOption.APPEND);
        indexFiles.add(indexFile.toString());
    }

    private int getIndex(String startDir, String[] directChildDirs) {
        return Arrays.binarySearch(directChildDirs, startDir);
    }

    private String[] getSortedChildDirs(String currDir) throws IOException {
        String[] directChildDirs = Files.list(Paths.get(dbPath + currDir))
                .filter(Files::isDirectory)
                .map(path -> path.getFileName().toString())
                .toArray(String[]::new);
        Arrays.sort(directChildDirs);

        return directChildDirs;
    }

    /**
     * @param pathString
     * @param dirs
     * @return array of paths to temp files containing KV-file paths
     */
    private String[] getFilesRecursively(String pathString, String[] dirs) throws IOException {
        Path indexPath = Paths.get(DATA_TRANSFER_INDEX_FOLDER);
        Files.createDirectories(indexPath);

        String[] indexFiles = new String[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            try {
                String indexFileName = StringUtils.removeChar(pathString, SEP.charAt(0)) + dirs[i];
                Path newIndexFile = Paths.get(indexPath.toString() + SEP + indexFileName);
                Files.deleteIfExists(newIndexFile);
                Path indexFile = Files.createFile(newIndexFile);

                Path child = Paths.get(dbPath + SEP + pathString + SEP + dirs[i] + SEP);
                Files.walkFileTree(child, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (!Files.isDirectory(file)) {
                            Files.write(indexFile, file.toString().getBytes(), StandardOpenOption.APPEND);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
                indexFiles[i] = indexFile.toString();
            } catch (IOException ioe) {
                LOG.error(ioe);
                return new String[0];
            }
        }
        return indexFiles;
    }

    public boolean transfer(String[] indexFiles) throws IOException {
        if (moveDataSocket == null || moveDataSocket.isClosed() || !moveDataSocket.isConnected())
            initSocket();
        for (String indexFile : indexFiles) {
            List<String> filesToMove = Files.readAllLines(Paths.get(indexFile));
            for (String file : filesToMove) {
                if (!put(file))
                    return false;
            }
        }
        return true;
    }

    private boolean put(String file) throws IOException {
        K key = new K(HashUtils.getHashBytesOf(Paths.get(file).getFileName().toString()));
        V val = new V(Files.readAllBytes(Paths.get(file)));
        byte[] toSend = MessageMarshaller.marshall(new Message(IMessage.Status.PUT, key, val));

        try {
            bos.write(toSend);
            bos.flush();
            LOG.info("sending " + toSend.length + " bytes to server");
        } catch (IOException e) {
            disconnect();
            LogUtils.printLogError(LOG, e, "Could't connect to the server. Disconnecting...");
            return false;
        }
        IMessage response = MessageMarshaller.unmarshall(receive());
        LOG.info("Received from server: " + response);
        return true;
    }

    private byte[] receive() {
        byte[] data = new byte[1 + 3 + 16 + 1024 * 120];
        try {
            moveDataSocket.setSoTimeout(5000);
            bis = new BufferedInputStream(moveDataSocket.getInputStream());
            int bytesCopied = bis.read(data);
            LOG.info("received data from server" + bytesCopied + " bytes");
        } catch (SocketTimeoutException ste) {
            LogUtils.printLogError(LOG, ste, "'receive' timeout. Client will disconnect from server.");
            disconnect();
        } catch (IOException e) {
            LogUtils.printLogError(LOG, e, "Could't connect to the server. Disconnecting...");
            disconnect();
        }
        return data;
    }

    public void connect() throws IOException {
        try {
            moveDataSocket = new Socket();
            moveDataSocket.connect(new InetSocketAddress(target.getHost(), target.getPort()), 5000);
        } catch (UnknownHostException uhe) {
            throw LogUtils.printLogError(LOG, uhe, "Unknown host");
        } catch (SocketTimeoutException ste) {
            throw LogUtils.printLogError(LOG, ste, "Could not connect to server. Connection timeout.");
        } catch (IOException ioe) {
            throw LogUtils.printLogError(LOG, ioe, "Could not connect to server.");
        }
    }

    private void disconnect() {
        try {
            if (bos != null)
                bos.close();
            if (bis != null)
                bis.close();
            if (moveDataSocket != null) {
                moveDataSocket.close();
            }
            moveDataSocket = new Socket();
        } catch (IOException e) {
            LogUtils.printLogError(LOG, e, "Connection is already closed.");
        }
    }

    public static String getDataTransferIndexFolder() {
        return DATA_TRANSFER_INDEX_FOLDER;
    }
}
