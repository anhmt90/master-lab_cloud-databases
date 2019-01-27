package client.mapreduce;

import client.api.Client;
import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.client.MRKeyComparator;
import mapreduce.client.OutputCollector;
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

    public Job getJob() {
        return job;
    }

    public Metadata getMetadata() {
        return client.getMetadata();
    }

    public ConcurrentHashMap<String, String> getOutputs() {
        return outputs;
    }


    public void exec(Job job) {
        this.job = job;
        map();
        reduce();
        collectOutput();

        Validate.notNull(outputs, "Output is null");
        System.out.println(outputs.entrySet());
    }


    private void map() {
        outputs = new ConcurrentHashMap<>();
        startStatusReceiver();

        job.setJobIdBeforeMap();

        Task mapTask = new Task(TaskType.MAP, job);
        broadcast(mapTask);
        waitForCompletetion();

        job.setStep(Step.REDUCE);
        job.setJobIdAfterMap();
    }


    private void reduce() {
        outputs = new ConcurrentHashMap<>();
        startStatusReceiver();

        Task reduceTask = new Task(TaskType.REDUCE, job);
        multicast(reduceTask, getMulticastNodes());
        waitForCompletetion();

        job.setStep(Step.COLLECT);
        job.setJobIdAfterReduce();
    }

    private void collectOutput() {
        outputs = new ConcurrentHashMap<>();
        // startOuputCollector();

        Set<NodeInfo> nodes = getMulticastNodes();
        OutputCollector outputCollector = new OutputCollector(client);
        outputCollector.collect();
    }


    private void startStatusReceiver() {
        statusReceiver = new StatusReceiver(this);
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

        TreeSet<String> inputs = new TreeSet<>(new MRKeyComparator(getMetadata()));
        inputs.addAll(outputs.keySet());
        Iterator<String> iter = inputs.iterator();
        NodeInfo currNode = null;
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

}