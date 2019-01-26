package client.mapreduce;

import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.Task;
import mapreduce.common.TaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.TaskMessage;
import util.Validate;

import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import static protocol.Constants.MR_TASK_RECEIVER_PORT_DISTANCE;

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
        map();
//        reduce();

        Validate.notNull(outputs, "Output is null");
        System.out.println(outputs.entrySet());
    }

    private void map() {
        outputs = new ConcurrentHashMap<>();
        startOutputCollector();
        Task mapTask = new Task(TaskType.MAP, job);
        broadcast(mapTask);
        waitForCompletetion();

        job.setInput(new HashSet<>(outputs.keySet()));
        job.setStep(Step.REDUCE);
    }

    private void reduce() {
        outputs = new ConcurrentHashMap<>();
        startOutputCollector();
        Task reduceTask = new Task(TaskType.REDUCE, job);
        broadcast(reduceTask);
        waitForCompletetion();
    }

    private void startOutputCollector() {
        collector = new OutputCollector(this);
        collectorThread = new Thread(collector);
        collectorThread.start();
    }

    private void waitForCompletetion() {
        if (collectorThread.isAlive()) {
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

        for (NodeInfo node : metadata.get()) {
            try {
                TaskMessage taskMessage = new TaskMessage(task, collector.getCallbackInfo());
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

    public Job getJob() {
        return job;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public ConcurrentHashMap<String, String> getOutputs() {
        return outputs;
    }
}