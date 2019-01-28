package mapreduce.server;

import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.Task;
import mapreduce.common.TaskType;
import mapreduce.server.inverted_index.InvertedIndexMapper;
import mapreduce.server.inverted_index.InvertedIndexReducer;
import mapreduce.server.word_count.WordCountMapper;
import mapreduce.server.word_count.WordCountReducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.CallbackInfo;
import protocol.mapreduce.StatusMessage;
import protocol.mapreduce.TaskMessage;
import protocol.mapreduce.Utils;
import server.app.Server;
import util.LogUtils;
import util.Validate;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.Set;

import static protocol.Constants.MAX_TASK_MESSAGE_LENGTH;
import static protocol.Constants.MR_TASK_HANDLER_PORT_DISTANCE;

public class TaskHandler implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private DatagramPacket taskPacket;
    private Task task;
    private String currJobId;

    private Socket outputOutboundSocket;
    private BufferedOutputStream bos;
    private Server server;
    private String dbPath;
    private KeyHashRange appliedRange;

    private Reducer reducer;
    private Mapper mapper;
    private OutputWriter outputWriter;


    public TaskHandler(DatagramPacket taskPacket, Server server) {
        this.taskPacket = taskPacket;
        this.server = server;
    }

    public String getCurrJobId() {
        return currJobId;
    }

    public Task getTask() {
        return task;
    }

    public TaskType getTaskType() {
        return task.getTaskType();
    }

    public String getNodeId() {
        return server.getServerId();
    }

    public Metadata getMetadata() {
        return server.getMetadata();
    }

    NodeInfo getFirstNodeFromMetadata() {
        return server.getMetadata().get(0);
    }


    private void setPathAndRange(KeyHashRange taskRange) {
        dbPath = server.getCacheManager().getPersistenceManager().getDbPath();
        appliedRange = (taskRange == null) ? server.getWriteRange() : taskRange;
    }

    public void setCurrJobId(String currJobId) {
        this.currJobId = currJobId;
    }

    @Override
    public void run() {
        Validate.isTrue(taskPacket.getLength() <= MAX_TASK_MESSAGE_LENGTH, "taskPacket's length is " + taskPacket.getLength());
        while (true) {
            TaskMessage taskMessage = MessageSerializer.deserialize(taskPacket.getData());
            LOG.info("CallbackInfo: " + taskMessage.getCallback());
            try {
                connect(taskMessage.getCallback());
            } catch (IOException e) {
                LOG.error(e);
                if (e instanceof BindException) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        LOG.error(e1);
                    }
                    continue;
                }
                break;
            }
            try {
                task = taskMessage.getTask();
                setPathAndRange(task.getAppliedRange());
                LOG.info("Current Path:" + dbPath);
                setCurrJobId(task.getJobId());
                LOG.info("Current JobId: " + currJobId);

                HashSet<String> outputKeys = new HashSet<>();
                boolean success = true;
                try {
                    switch (getTaskType()) {
                        case MAP:
                            startMapper();
                            currJobId = Utils.updateJobIdAfterMap(currJobId);
                            startWriter(mapper);
                            outputKeys = mapper.getKeySet();
                            break;
                        case REDUCE:
                            startReducer();
                            currJobId = Utils.updateJobIdAfterReduce(currJobId);
                            startWriter(reducer);
                            outputKeys = reducer.getKeySet();
                            break;
                        default:
                            throw new IllegalArgumentException("Undefined task type!");
                    }
                } catch (RuntimeException e) {
                    LOG.error(e);
                    success = false;
                }
                LOG.info("Preparing status message to send back");
                StatusMessage statusMessage = new StatusMessage(getTaskType(), success, outputKeys);

                if (!outputOutboundSocket.isConnected())
                    connect(taskMessage.getCallback());

                send(statusMessage);
                outputOutboundSocket.close();
            } catch (RuntimeException e) {
                LOG.error(e);
            } catch (IOException e) {
                LOG.error(e);
            }
            break;
        }
        LOG.info("TaskHandler terminates " + task.getTaskType() + " task");
    }

    private void startMapper() {
        LOG.info("Starts " + task.getAppId() + " Mapper");
        switch (task.getAppId()) {
            case WORD_COUNT:
                startWordCountMapper();
                break;
            case INVERTED_INDEX:
                startInvertedIndexMapper();
                break;
            default:
                throw new IllegalArgumentException("Undefined Application ID!");
        }
    }


    private void startReducer() {
        LOG.info("Starts " + task.getAppId() + " Reducer");
        switch (task.getAppId()) {
            case WORD_COUNT:
                startWordCountReducer();
            case INVERTED_INDEX:
                startInvertedIndexreducer();
                break;
            default:
                throw new IllegalArgumentException("Undefined Application ID!");
        }
    }

    private void startWordCountMapper() {
        mapper = new WordCountMapper(dbPath, appliedRange);
        mapper.map();
    }

    private void startInvertedIndexMapper() {
        mapper = new InvertedIndexMapper(dbPath, appliedRange, task.getInput());
        mapper.map();
    }

    private void startWordCountReducer() {
        reducer = new WordCountReducer(dbPath, appliedRange, currJobId);
        reducer.reduce();
    }

    private void startInvertedIndexreducer() {
        reducer = new InvertedIndexReducer(dbPath, appliedRange, currJobId);
        reducer.reduce();
    }

    private void startWriter(MapReduce mapperReducer) {
        outputWriter = new OutputWriter<>(mapperReducer, this);
        outputWriter.write();
    }

    public void connect(CallbackInfo callback) throws IOException {
        try {
            outputOutboundSocket = new Socket();
            outputOutboundSocket.setReuseAddress(true);
            outputOutboundSocket.bind(new InetSocketAddress(server.getServicePort() + MR_TASK_HANDLER_PORT_DISTANCE));
            outputOutboundSocket.connect(new InetSocketAddress(callback.getResponseAddress(), callback.getResponsePort()), 5000);
        } catch (UnknownHostException uhe) {
            LOG.error("Unknown host", uhe);
        } catch (SocketTimeoutException ste) {
            LOG.error("Could not connect to Coordinator. Connection timeout.", ste);
        } catch (IOException ioe) {
            LOG.error("Could not connect to Coordinator.", ioe);
        }
    }

    /**
     * sends a Failure report to the connected ECS
     *
     * @param message the report message that needs to be sent
     * @throws IOException
     */
    public void send(StatusMessage message) throws IOException {
        try {
            bos = new BufferedOutputStream(outputOutboundSocket.getOutputStream());
            byte[] bytes = MessageSerializer.serialize(message);
            bos.write(bytes);
            bos.flush();

            LOG.info("SEND \t<"
                    + outputOutboundSocket.getInetAddress().getHostAddress() + ":"
                    + outputOutboundSocket.getPort() + ">: '"
                    + message.toString() + "'");

        } catch (IOException e) {
            LOG.error(e);
            throw e;
        }
    }

}
