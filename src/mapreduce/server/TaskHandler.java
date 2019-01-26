package mapreduce.server;

import client.api.Client;
import ecs.KeyHashRange;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.IMessage;
import protocol.kv.K;
import protocol.kv.Message;
import protocol.kv.V;
import protocol.mapreduce.CallbackInfo;
import protocol.mapreduce.OutputMessage;
import protocol.mapreduce.TaskMessage;
import server.app.Server;
import util.FileUtils;
import util.HashUtils;
import util.LogUtils;
import util.Validate;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.file.Paths;
import java.util.Map;

import static protocol.Constants.MAX_TASK_MESSAGE_LENGTH;
import static protocol.Constants.MR_TASK_HANDLER_PORT_DISTANCE;
import static server.api.BatchDataTransferProcessor.getHashedKeyFromFileName;

public class TaskHandler implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private DatagramPacket taskPacket;
    private Socket outputOutboundSocket;
    private BufferedOutputStream bos;
    private Server server;
    private String dbPath;
    private KeyHashRange appliedRange;


    public TaskHandler(DatagramPacket taskPacket, Server server) {
        this.taskPacket = taskPacket;
        this.server = server;


    }

    @Override
    public void run() {
        Validate.isTrue(taskPacket.getLength() <= MAX_TASK_MESSAGE_LENGTH, "taskPacket's length is " + taskPacket.getLength());
        try {
            TaskMessage taskMessage = MessageSerializer.deserialize(taskPacket.getData());
            connect(taskMessage.getCallback());
            Task task = taskMessage.getTask();
            setPathAndRange(task.getAppliedRange());

            switch (task.getTaskType()) {
                case MAP:
                    startMapper(task);
                    break;
                case REDUCE:
                    break;
                default:
                    throw new IllegalArgumentException("Undefined task type!");
            }

//            HashMap<String, String> hm = new HashMap<>();
//            hm.put("foo", "bar");
//            send(new OutputMessage(TaskType.MAP, hm));

            outputOutboundSocket.close();
        } catch (IOException e) {
            LOG.error(e);
        }

    }

    private void setPathAndRange(KeyHashRange taskRange) {
        dbPath = server.getCacheManager().getPersistenceManager().getDbPath();
        appliedRange = (taskRange != null) ? server.getWriteRange() : taskRange;
    }

    private void startMapper(Task task) {
        try {
            switch (task.getAppId()) {
                case WORD_COUNT:
                    startWordCountMapping();
                    break;
                default:
                    throw new IllegalArgumentException("Undefined Application ID!");
            }
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    private void startWordCountMapping() throws IOException {
        WordCountMapper wcMapper = new WordCountMapper(dbPath, appliedRange);
        wcMapper.map();

        OutputWriter<String, Integer> writer = new OutputWriter<>(wcMapper.getOutput(), getFirstNodeFromMetadata());
        writer.write();
    }

    private NodeInfo getFirstNodeFromMetadata() {
        return server.getMetadata().get(0);
    }


    public void connect(CallbackInfo callback) throws IOException {
        try {
            outputOutboundSocket = new Socket();
            outputOutboundSocket.bind(new InetSocketAddress(server.getServicePort() + MR_TASK_HANDLER_PORT_DISTANCE));
            outputOutboundSocket.connect(new InetSocketAddress(callback.getResponseAddress(), callback.getResponsePort()), 5000);
        } catch (UnknownHostException uhe) {
            throw LogUtils.printLogError(LOG, uhe, "Unknown host");
        } catch (SocketTimeoutException ste) {
            throw LogUtils.printLogError(LOG, ste, "Could not connect to Coordinator. Connection timeout.");
        } catch (IOException ioe) {
            throw LogUtils.printLogError(LOG, ioe, "Could not connect to Coordinator.");
        }
    }

    /**
     * sends a Failure report to the connected ECS
     *
     * @param message the report message that needs to be sent
     * @throws IOException
     */
    public void send(OutputMessage message) throws IOException {
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
