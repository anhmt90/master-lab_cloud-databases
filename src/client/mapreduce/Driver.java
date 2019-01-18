package client.mapreduce;

import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.Task;
import mapreduce.common.TaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.Constants;
import protocol.mapreduce.TaskMessage;
import util.Validate;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static protocol.Constants.MR_PORT_DISTANCE;

public class Driver {
    public static final String MAPREDUCE_LOG = "mapreduce";
    private static Logger LOG = LogManager.getLogger(MAPREDUCE_LOG);

    private static final int SOCKET_TIMEOUT = 60000;

    /**
     * List of the storage servers with their addresses
     */
    private Metadata metadata;
    private DatagramSocket taskDeliverySocket;

    private OutputCollector collector;
    private Thread collectorThread;
    private Job job;

    /**
     * contains keys of key-value pairs emitted from the map task
     */
//    private ConcurrentHashMap<String, String> intermediateOutputs;

    /**
     * contains key-value pairs emitted from the reduce task
     */
    private ConcurrentHashMap<String, String> outputs;


    public Driver(Metadata metadata) {
        Validate.notNull(metadata == null, "Metadata is null");
        this.metadata = metadata;
        initTaskDeliverySocket();
    }


    public void exec(Job job) {
        this.job = job;
        startMap();
        startReduce();
    }

    private void startMap() {
        outputs = new ConcurrentHashMap<>();
        startOutputCollector();
        Task mapTask = new Task(TaskType.MAP, job.getApplicationID(), job.getInput());
        broadcast(mapTask);
        waitForCompletetion();

        job.setInput(new HashSet<>(outputs.keySet()));
        job.setStep(Step.REDUCE);
    }

    private void startReduce() {
        outputs = new ConcurrentHashMap<>();
        startOutputCollector();
        Task reduceTask = new Task(TaskType.REDUCE, job.getApplicationID(), job.getInput());
        broadcast(reduceTask);
        waitForCompletetion();
    }

    private void startOutputCollector() {
        collector = new OutputCollector(this);
        collectorThread = new Thread(collector);
        collectorThread.start();
    }

    private void waitForCompletetion() {
        if(collectorThread.isAlive()) {
            try {
                collectorThread.join();
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
        Validate.notNull(collector.getCallbackInfo(), "callbackInfo is null");
        Validate.notNull(task, "task is null");
        Validate.isTrue(collector.getExpectedConnections().isEmpty(), "expectedConnections is not empty");

        collector.setExpectedConnections(metadata.get().stream().collect(Collectors.toMap(NodeInfo::getHost, NodeInfo::getId)));
        while (taskDeliverySocket != null && !taskDeliverySocket.isClosed()) {
            for (NodeInfo node : metadata.get()) {
                try {
                    TaskMessage taskMessage = new TaskMessage(task, collector.getCallbackInfo());
                    byte[] serialized = MessageSerializer.serialize(taskMessage);
                    DatagramPacket packet = new DatagramPacket(serialized, serialized.length,
                            InetAddress.getByName(node.getHost()), node.getPort() + MR_PORT_DISTANCE);
                    
                    LOG.info("Sending " + task + " to <" + node.getHost() + ":" + node.getPort() + ">");
                    taskDeliverySocket.send(packet);

                } catch (IOException e) {
                    LOG.error(e);
                }

            }
        }
    }

    public Job getJob() {
        return job;
    }

//    public HashSet<String> getIntermediateOutputs() {
//        return intermediateOutputs;
//    }

    public ConcurrentHashMap<String, String> getOutputs() {
        return outputs;
    }
}