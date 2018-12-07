package testing;

import ecs.KeyHashRange;
import ecs.NodeInfo;
import junit.framework.TestCase;
import org.junit.Test;
import server.api.BatchDataTransferProcessor;
import server.storage.disk.PersistenceManager;
import util.HashUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import static util.FileUtils.SEP;

public class FetchBatchDataTest extends TestCase {

    static final String NODE_1 = "node1";
    static final String NODE_2 = "node2";
    static final String NODE_3 = "node3";

    private PersistenceManager pm1;
    private PersistenceManager pm2;
    private PersistenceManager pm3;

    private Method indexRelevantDataFiles;
    private Method cleanUp;
    private String[] keySet1;
    private String[] keySet2;
    private KeyHashRange node1_range;
    private KeyHashRange node2_range;
    private KeyHashRange node3_range;


    public void prepare() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        pm1 = new PersistenceManager("db_test" + SEP + NODE_1);
        pm2 = new PersistenceManager("db_test" + SEP + NODE_2);
        pm3 = new PersistenceManager("db_test" + SEP + NODE_3);

        String node1Hash = new String(new char[8]).replace("\0", "3333");
        String node2Hash = new String(new char[8]).replace("\0", "9999");
        String node3Hash = new String(new char[8]).replace("\0", "eeee");

        node1_range = new KeyHashRange(HashUtils.increaseHashBy1(node3Hash), node1Hash); // EEEE..EEEF - 3333..3333
        node2_range = new KeyHashRange(HashUtils.increaseHashBy1(node1Hash), node2Hash); // 3333..3334 - 9999..9999
        node3_range = new KeyHashRange(HashUtils.increaseHashBy1(node2Hash), node3Hash); // 9999..999A - EEEE..EEEE

        populateData();
    }

    @Test
    public void test_fetching_data_to_transfer_when_removing_node() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
        prepare();

        node3_range = new KeyHashRange(HashUtils.increaseHashBy1(node1_range.getEnd()), node3_range.getEnd());
        NodeInfo target = new NodeInfo(NODE_3, "127.0.0.1", 50000, node3_range);
        BatchDataTransferProcessor batchProcessor = new BatchDataTransferProcessor(target, pm2.getDbPath());
        setIndexRelevantDataFilesMethod(batchProcessor);
        String[] indexFiles = (String[]) indexRelevantDataFiles.invoke(batchProcessor, node3_range);

        List<String> filesToTransfer = new ArrayList<>();
        for (String file : indexFiles) {
            filesToTransfer.addAll(Files.readAllLines(Paths.get(file)));
        }
        assertThat(filesToTransfer.size() == keySet2.length, is(true));
        List<String> keySet2_toMove = Arrays.asList(keySet2);
        for (String file : filesToTransfer) {
            String fileName = Paths.get(file).getFileName().toString();
            assertThat(keySet2_toMove.contains(fileName), is(true));
        }

        deleteIndexFolder(batchProcessor, indexFiles);
    }

    @Test
    public void test_fetching_data_to_transfer_when_adding_new_node_() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException {
        prepare();

        // adding new node between node3 and node1
        String newNodeHash = "10" + new String(new char[6]).replace("\0", "00000");
        KeyHashRange newNode_range = new KeyHashRange(HashUtils.increaseHashBy1(node3_range.getEnd()), newNodeHash);

        // update range of successor (node1)
        node1_range = new KeyHashRange(HashUtils.increaseHashBy1(newNodeHash), node1_range.getEnd());
        NodeInfo target = new NodeInfo(NODE_1, "127.0.0.1", 50000, node1_range);
        BatchDataTransferProcessor batchProcessor = new BatchDataTransferProcessor(target, pm1.getDbPath());

        setIndexRelevantDataFilesMethod(batchProcessor);
        String[] indexFiles = (String[]) indexRelevantDataFiles.invoke(batchProcessor, newNode_range);

        List<String> filesToTransfer = new ArrayList<>();
        for (String file : indexFiles) {
            filesToTransfer.addAll(Files.readAllLines(Paths.get(file)));
        }

        List<String> keySet1_toMove = Arrays.asList(Arrays.copyOfRange(keySet1, 0, 5));
        for (String file : filesToTransfer) {
            String fileName = Paths.get(file).getFileName().toString();
            assertThat(keySet1_toMove.contains(fileName), is(true));
        }
        deleteIndexFolder(batchProcessor, indexFiles);

    }

    private void setIndexRelevantDataFilesMethod(BatchDataTransferProcessor batchProcessor) throws NoSuchMethodException {
        indexRelevantDataFiles = batchProcessor.getClass().getDeclaredMethod("indexRelevantDataFiles", KeyHashRange.class);
        indexRelevantDataFiles.setAccessible(true);
    }

    private void setCleanUpMethod(BatchDataTransferProcessor batchProcessor) throws NoSuchMethodException {
        cleanUp = batchProcessor.getClass().getDeclaredMethod("cleanUp", String[].class);
        cleanUp.setAccessible(true);
    }

    private void deleteIndexFolder(BatchDataTransferProcessor batchProcessor, String[] indexFiles) throws IOException {
        for (String file : indexFiles) {
            Files.deleteIfExists(Paths.get(file));
        }
        Files.deleteIfExists(Paths.get(batchProcessor.getDataTransferIndexFolder()));
    }


    private void populateData() {
        keySet1 = new String[]{
                new String(new char[6]).replace("\0", "eeeee") + "ef", //should be moved
                new String(new char[8]).replace("\0", "ffff"), //should be moved
                "f" + HashUtils.getHash("key1").substring(1), //should be moved
                "f" + HashUtils.getHash("key2").substring(1), //should be moved
                "ffff" + HashUtils.getHash("key6").substring(4), //should be moved
                "eeee" + HashUtils.getHash("key5").substring(4),
                "2" + HashUtils.getHash("key7").substring(1),
                "2" + HashUtils.getHash("key8").substring(1)
        };

        for (String key : keySet1)
            pm1.write(key, key.getBytes());

        keySet2 = new String[]{
                "4" + HashUtils.getHash("key9").substring(1),
                "4" + HashUtils.getHash("key10").substring(1),
                "5" + HashUtils.getHash("key11").substring(1),
                "5" + HashUtils.getHash("key12").substring(1),
                "6" + HashUtils.getHash("key13").substring(1),
                "6" + HashUtils.getHash("key14").substring(1),
                "7" + HashUtils.getHash("key15").substring(1),
                "8" + HashUtils.getHash("key16").substring(1)
        };
        for (String key : keySet2)
            pm2.write(key, key.getBytes());

        String[] keySet3 = new String[]{
                "a" + HashUtils.getHash("key17").substring(1),
                "a" + HashUtils.getHash("key18").substring(1),
                "b" + HashUtils.getHash("key19").substring(1),
                "b" + HashUtils.getHash("key20").substring(1),
                "c" + HashUtils.getHash("key21").substring(1),
                "c" + HashUtils.getHash("key22").substring(1),
                "d" + HashUtils.getHash("key23").substring(1),
                "d" + HashUtils.getHash("key24").substring(1)
        };
        for (String key : keySet3)
            pm3.write(key, key.getBytes());
    }
}
