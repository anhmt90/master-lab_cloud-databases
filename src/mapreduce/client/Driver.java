package mapreduce.client;

import client.api.Client;
import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.Task;
import mapreduce.common.TaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.TaskMessage;
import util.HashUtils;
import util.Validate;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static protocol.Constants.MR_TASK_RECEIVER_PORT_DISTANCE;

public class Driver {
    public static final String MAPREDUCE_LOG = "mapreduce";
    private static Logger LOG = LogManager.getLogger(MAPREDUCE_LOG);

    private static final int SOCKET_TIMEOUT = 60000;

    /**
     * The client calling this Driver
     */
    private Client client;

    private DatagramSocket taskDeliverySocket;

    private StatusReceiver statusReceiver;
    private Thread statusReceiverThread;
    private Job job;

    /**
     * contains key-value pairs emitted from the reduce task
     */
    private ConcurrentHashMap<String, String> outputs;


    public Driver(Client client) {
        this.client = client;
        Validate.notNull(client.getMetadata() == null, "Metadata is null");
        initTaskDeliverySocket();
    }

    String getJobId(){
        return job.getJobId();
    }

    public Metadata getMetadata() {
        return client.getMetadata();
    }

    public ConcurrentHashMap<String, String> getOutputs() {
        return outputs;
    }

    public void setOutputs(ConcurrentHashMap<String, String> outputs) {
        this.outputs = outputs;
    }

    public void exec(Job job) {
        this.job = job;
        map();
        reduce();
        collect();

        Validate.notNull(outputs, "Output is null");
    }


    private void map() {
        outputs = new ConcurrentHashMap<>();
        startStatusReceiver(getMetadata().get());

        job.updateJobIdBeforeMap();
        LOG.info("Current JobId: " + getJobId());

        Task mapTask = new Task(TaskType.MAP, job);
        broadcast(mapTask);
        waitForCompletetion();

        job.setStep(Step.REDUCE);
        job.updateJobIdAfterMap();
        LOG.info("JobId updated: " + getJobId());
    }


    private void reduce() {
        Set<NodeInfo> nodes = getMulticastNodes();
        outputs = new ConcurrentHashMap<>();

        startStatusReceiver(new ArrayList<>(nodes));
        LOG.info("Current JobId: " + getJobId());

        Task reduceTask = new Task(TaskType.REDUCE, job);
        multicast(reduceTask, nodes);
        waitForCompletetion();

        job.setStep(Step.COLLECT);
        job.updateJobIdAfterReduce();
        LOG.info("JobId updated: " + getJobId());
    }

    private void collect() {
        // startOuputCollector();

        OutputCollector outputCollector = new OutputCollector(client, this);
        outputCollector.collect();
    }


    private void startStatusReceiver(List<NodeInfo> expectedConnections) {
        statusReceiver = new StatusReceiver(this, expectedConnections);
        statusReceiverThread = new Thread(statusReceiver);
        statusReceiverThread.start();
    }

    private void waitForCompletetion() {
        if (statusReceiverThread.isAlive()) {
            try {
                statusReceiverThread.join();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    private void initTaskDeliverySocket() {
        try {
            taskDeliverySocket = new DatagramSocket();

        } catch (SocketException e) {
            LOG.warn(e);
        }
    }

    private void broadcast(Task task) {
        multicast(task, getMetadata().getOrdered());
    }

    private void multicast(Task task, Set<NodeInfo> nodes) {
        Validate.notNull(statusReceiver.getCallbackInfo(), "callbackInfo is null");
        Validate.notNull(task, "task is null");
        LOG.info("Target nodes: " + Arrays.toString(nodes.toArray()));

        for (NodeInfo node : nodes) {
            try {
                TaskMessage taskMessage = new TaskMessage(task, statusReceiver.getCallbackInfo());
                byte[] serialized = MessageSerializer.serialize(taskMessage);
                DatagramPacket taskPacket = new DatagramPacket(serialized, serialized.length,
                        InetAddress.getByName(node.getHost()), node.getPort() + MR_TASK_RECEIVER_PORT_DISTANCE);

                LOG.info("Sending " + task + "(" + taskPacket.getLength() / 1024.0 + " KB)" + " to <" + node.getHost() + ":" + node.getPort() + ">");
                taskDeliverySocket.send(taskPacket);

            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }

    private Set<NodeInfo> getMulticastNodes() {
        HashSet<NodeInfo> nodes = new HashSet<>();
        NodeInfo currNode = null;
        LOG.info("Current output key set: " + Arrays.toString(getKeys().toArray()));
        Iterator<String> iter = getKeys().iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            String hashed = HashUtils.hash(key);
            if (currNode != null && currNode.getWriteRange().contains(hashed))
                continue;
            currNode = getMetadata().getCoordinator(hashed);
            nodes.add(currNode);
        }
        return nodes;
    }

    ArrayList<String> getKeys() {
//        TreeSet<String> keySet = new TreeSet<>(new MRKeyComparator(getMetadata()));
        ArrayList<String> keys = new ArrayList<>();
        keys.addAll(outputs.keySet());
        keys.sort(new MRKeyComparator(getMetadata()));
        return keys;
    }

}