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

import static util.FileUtils.SEP;

public class BatchDataTransferProcessor {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    /**
     * The socket being used to move data when adding/removing servers
     */
    private Socket moveDataSocket;
    BufferedOutputStream bos;
    BufferedInputStream bis;

    NodeInfo target;

    public BatchDataTransferProcessor(NodeInfo target) throws IOException {
        this.target = target;
        connect();
        bos = new BufferedOutputStream(moveDataSocket.getOutputStream());
        bis = new BufferedInputStream(moveDataSocket.getInputStream());
    }

    public boolean handleTransferData(KeyHashRange range) {
        if (range.isWrappedAround()) {
            KeyHashRange leftRange = new KeyHashRange(range.getStart(), new String(new char[4]).replace("\0", "FFFF"));
            KeyHashRange rightRange = new KeyHashRange(new String(new char[4]).replace("\0", "0000"), range.getEnd());
            return handleTransferData(leftRange) && handleTransferData(rightRange);
        }

        String[] start = StringUtils.splitEvery(range.getStart(), 2);
        String[] end = StringUtils.splitEvery(range.getEnd(), 2);
        String commonPrefix = StringUtils.getLongestCommonPrefix(range.getStart(), range.getEnd());

        String startDir = start[commonPrefix.length() / 2];
        String endDir = end[commonPrefix.length() / 2];

        String commonParent = StringUtils.joinSeparated(Arrays.copyOfRange(start, 0, commonPrefix.length() / 2 - 1), SEP);
        try {
            String[] directChildDirs = getSortedChildDirs(commonParent);
            int lowerBoundIndex = Arrays.binarySearch(directChildDirs, startDir);
            if (lowerBoundIndex == directChildDirs.length)
                return true;
            int upperBoundIndex = Arrays.binarySearch(directChildDirs, endDir);
            String[] middleFullRangeDirs = Arrays.copyOfRange(directChildDirs, lowerBoundIndex + 1, upperBoundIndex);
            String[] indexFiles = getFilesRecursively(commonParent, middleFullRangeDirs);

            for (int i = commonPrefix.length() / 2 + 1; i < start.length; i++) {
                String[] startIndexFiles = walkStart(Arrays.copyOfRange(start, 0, i), start[i]);
                String[] endIndexFiles = walkEnd(Arrays.copyOfRange(end, 0, i), end[i]);
                indexFiles = merge(indexFiles, startIndexFiles, endIndexFiles);
            }
            return transfer(indexFiles);

        } catch (IOException ioe) {
            return LogUtils.exitWithError(LOG, ioe);
        }
    }

    private String[] merge(String[] indexFiles, String[] startIndexFiles, String[] endIndexFiles) {
        String[] totalIndexFiles = new String[indexFiles.length + startIndexFiles.length + endIndexFiles.length];
        System.arraycopy(indexFiles, 0, totalIndexFiles, 0, indexFiles.length);
        System.arraycopy(startIndexFiles, 0, totalIndexFiles, indexFiles.length, startIndexFiles.length);
        System.arraycopy(endIndexFiles, 0, totalIndexFiles, indexFiles.length + startIndexFiles.length, endIndexFiles.length);
        indexFiles = totalIndexFiles;
        totalIndexFiles = null;
        return indexFiles;
    }

    private String[] walkStart(String[] parentDirs, String lowerBound) throws IOException {
        String currDir = StringUtils.joinSeparated(parentDirs, SEP);
        String[] directChildDirs = getSortedChildDirs(currDir);
        int lowerBoundIndex = Arrays.binarySearch(directChildDirs, lowerBound);
        if (lowerBoundIndex == directChildDirs.length)
            return new String[0];
        String[] fullRangeDirs = Arrays.copyOfRange(directChildDirs, lowerBoundIndex + 1, directChildDirs.length);
        return getFilesRecursively(currDir, fullRangeDirs);
    }

    private String[] walkEnd(String[] parentDirs, String upperBound) throws IOException {
        String currDir = StringUtils.joinSeparated(parentDirs, SEP);
        String[] directChildDirs = getSortedChildDirs(currDir);
        int upperBoundIndex = Arrays.binarySearch(directChildDirs, upperBound);
        if (upperBoundIndex == -1)
            return new String[0];
        String[] fullRangeDirs = Arrays.copyOfRange(directChildDirs, 0, upperBoundIndex);
        return getFilesRecursively(currDir, fullRangeDirs);
    }

    private String[] getSortedChildDirs(String currDir) throws IOException {
        String[] directChildDirs = Files.list(Paths.get(currDir))
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
    private String[] getFilesRecursively(String pathString, String[] dirs) {
        String[] indexFiles = new String[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            try {
                String indexFileName = StringUtils.removeChar(pathString, SEP.charAt(0)) + dirs[i];
                Path indexFile = Files.createFile(Paths.get(System.getProperty("user.dir") + SEP + "tmp" + SEP + indexFileName));

                Files.walkFileTree(Paths.get(pathString + dirs[i] + SEP), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (!attrs.isDirectory()) {
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
            LogUtils.printLogError(e, "Could't connect to the server. Disconnecting...", LOG);
            return false;
        }
        IMessage response = MessageMarshaller.unmarshall(receive());
        LOG.info("Received from server: " + response);
        return true;
    }

    private byte[] receive() {
        byte[] data = new byte[2 + 20 + 1024 * 120];
        try {
            moveDataSocket.setSoTimeout(5000);
            bis = new BufferedInputStream(moveDataSocket.getInputStream());
            int bytesCopied = bis.read(data);
            LOG.info("received data from server" + bytesCopied + " bytes");
        } catch (SocketTimeoutException ste) {
            LogUtils.printLogError(ste, "'receive' timeout. Client will disconnect from server.", LOG);
            disconnect();
        } catch (IOException e) {
            LogUtils.printLogError(e, "Could't connect to the server. Disconnecting...", LOG);
            disconnect();
        }
        return data;
    }

    public void connect() throws IOException {
        try {
            moveDataSocket = new Socket();
            moveDataSocket.connect(new InetSocketAddress(target.getHost(), target.getPort()), 5000);
        } catch (UnknownHostException uhe) {
            throw LogUtils.printLogError(uhe, "Unknown host", LOG);
        } catch (SocketTimeoutException ste) {
            throw LogUtils.printLogError(ste, "Could not connect to server. Connection timeout.", LOG);
        } catch (IOException ioe) {
            throw LogUtils.printLogError(ioe, "Could not connect to server.", LOG);
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
            LogUtils.printLogError(e, "Connection is already closed.", LOG);
        }
    }
}
